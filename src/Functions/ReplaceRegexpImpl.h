#pragma once

#include <base/types.h>
#include <Columns/ColumnString.h>
#include <IO/WriteHelpers.h>

#include "config.h"
#include <re2_st/re2.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
}

struct ReplaceRegexpTraits
{
    enum class Replace
    {
        First,
        All
    };
};

/** Replace all matches of regexp 'needle' to string 'replacement'. 'needle' and 'replacement' are constants.
  * 'replacement' can contain substitutions, for example: '\2-\3-\1'
  */
template <typename Name, ReplaceRegexpTraits::Replace replace>
struct ReplaceRegexpImpl
{
    static constexpr auto name = Name::name;

    struct Instruction
    {
        /// If not negative, perform substitution of n-th subpattern from the regexp match.
        int substitution_num = -1;
        /// Otherwise, paste this literal string verbatim.
        String literal;

        explicit Instruction(int substitution_num_) : substitution_num(substitution_num_) {}
        explicit Instruction(String literal_) : literal(std::move(literal_)) {}
    };

    /// Decomposes the replacement string into a sequence of substitutions and literals.
    /// E.g. "abc\1de\2fg\1\2" --> inst("abc"), inst(1), inst("de"), inst(2), inst("fg"), inst(1), inst(2)
    using Instructions = std::vector<Instruction>;

    static constexpr int max_captures = 10;

    static Instructions createInstructions(std::string_view replacement, int num_captures)
    {
        Instructions instructions;

        String literals;
        for (size_t i = 0; i < replacement.size(); ++i)
        {
            if (replacement[i] == '\\' && i + 1 < replacement.size())
            {
                if (isNumericASCII(replacement[i + 1])) /// Substitution
                {
                    if (!literals.empty())
                    {
                        instructions.emplace_back(literals);
                        literals = "";
                    }
                    instructions.emplace_back(replacement[i + 1] - '0');
                }
                else
                    literals += replacement[i + 1]; /// Escaping
                ++i;
            }
            else
                literals += replacement[i]; /// Plain character
        }

        if (!literals.empty())
            instructions.emplace_back(literals);

        for (const auto & instr : instructions)
            if (instr.substitution_num >= num_captures)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Id {} in replacement string is an invalid substitution, regexp has only {} capturing groups",
                    instr.substitution_num, num_captures - 1);

        return instructions;
    }

    static void processString(
        const char * haystack_data,
        size_t haystack_length,
        ColumnString::Chars & res_data,
        ColumnString::Offset & res_offset,
        const re2_st::RE2 & searcher,
        int num_captures,
        const Instructions & instructions)
    {
        re2_st::StringPiece haystack(haystack_data, haystack_length);
        re2_st::StringPiece matches[max_captures];

        size_t copy_pos = 0;
        size_t match_pos = 0;

        while (match_pos < haystack_length)
        {
            /// If no more replacements possible for current string
            bool can_finish_current_string = false;

            if (searcher.Match(haystack, match_pos, haystack_length, re2_st::RE2::Anchor::UNANCHORED, matches, num_captures))
            {
                const auto & match = matches[0]; /// Complete match (\0)
                size_t bytes_to_copy = (match.data() - haystack.data()) - copy_pos;

                /// Copy prefix before current match without modification
                res_data.resize(res_data.size() + bytes_to_copy);
                memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], haystack.data() + copy_pos, bytes_to_copy);
                res_offset += bytes_to_copy;
                copy_pos += bytes_to_copy + match.length();
                match_pos = copy_pos;

                /// Substitute inside current match using instructions
                for (const auto & instr : instructions)
                {
                    std::string_view replacement;
                    if (instr.substitution_num >= 0)
                        replacement = std::string_view(matches[instr.substitution_num].data(), matches[instr.substitution_num].size());
                    else
                        replacement = instr.literal;
                    res_data.resize(res_data.size() + replacement.size());
                    memcpy(&res_data[res_offset], replacement.data(), replacement.size());
                    res_offset += replacement.size();
                }

                if constexpr (replace == ReplaceRegexpTraits::Replace::First)
                    can_finish_current_string = true;

                if (match.empty())
                {
                    /// Step one character to avoid infinite loop
                    ++match_pos;
                    if (match_pos >= haystack_length)
                        can_finish_current_string = true;
                }
            }
            else
                can_finish_current_string = true;

            /// If ready, append suffix after match to end of string.
            if (can_finish_current_string)
            {
                res_data.resize(res_data.size() + haystack_length - copy_pos);
                memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], haystack.data() + copy_pos, haystack_length - copy_pos);
                res_offset += haystack_length - copy_pos;
                copy_pos = haystack_length;
                match_pos = copy_pos;
            }
        }

        res_data.resize(res_data.size() + 1);
        res_data[res_offset] = 0;
        ++res_offset;
    }

    static void vectorConstantConstant(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offset,
        const String & needle,
        const String & replacement,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets)
    {
        if (needle.empty())
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Length of the pattern argument in function {} must be greater than 0.", name);

        ColumnString::Offset res_offset = 0;
        res_data.reserve(haystack_data.size());
        size_t haystack_size = haystack_offset.size();
        res_offsets.resize(haystack_size);

        re2_st::RE2::Options regexp_options;
        /// Don't write error messages to stderr.
        regexp_options.set_log_errors(false);

        re2_st::RE2 searcher(needle, regexp_options);

        if (!searcher.ok())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The pattern argument is not a valid re2 pattern: {}",
                searcher.error());

        int num_captures = std::min(searcher.NumberOfCapturingGroups() + 1, max_captures);

        Instructions instructions = createInstructions(replacement, num_captures);

        /// Cannot perform search for whole columns. Will process each string separately.
        for (size_t i = 0; i < haystack_size; ++i)
        {
            size_t from = i > 0 ? haystack_offset[i - 1] : 0;
            const char * hs_data = reinterpret_cast<const char *>(haystack_data.data() + from);
            const size_t hs_length = static_cast<unsigned>(haystack_offset[i] - from - 1);

            processString(hs_data, hs_length, res_data, res_offset, searcher, num_captures, instructions);
            res_offsets[i] = res_offset;
        }
    }

    template <typename... Args>
    static void vectorVectorConstant(Args && ...)
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Function '{}' doesn't support non-constant pattern arguments", name);
    }

    template <typename... Args>
    static void vectorConstantVector(Args && ...)
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Function '{}' doesn't support non-constant replacement arguments", name);
    }

    template <typename... Args>
    static void vectorVectorVector(Args && ...)
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Function '{}' doesn't support non-constant pattern or replacement arguments", name);
    }

    static void vectorFixedConstantConstant(
        const ColumnString::Chars & haystack_data,
        size_t n,
        const String & needle,
        const String & replacement,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets)
    {
        if (needle.empty())
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Length of the pattern argument in function {} must be greater than 0.", name);

        ColumnString::Offset res_offset = 0;
        size_t haystack_size = haystack_data.size() / n;
        res_data.reserve(haystack_data.size());
        res_offsets.resize(haystack_size);

        re2_st::RE2::Options regexp_options;
        /// Don't write error messages to stderr.
        regexp_options.set_log_errors(false);

        re2_st::RE2 searcher(needle, regexp_options);

        if (!searcher.ok())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The pattern argument is not a valid re2 pattern: {}",
                searcher.error());

        int num_captures = std::min(searcher.NumberOfCapturingGroups() + 1, max_captures);

        Instructions instructions = createInstructions(replacement, num_captures);

        for (size_t i = 0; i < haystack_size; ++i)
        {
            size_t from = i * n;
            const char * hs_data = reinterpret_cast<const char *>(haystack_data.data() + from);
            const size_t hs_length = n;

            processString(hs_data, hs_length, res_data, res_offset, searcher, num_captures, instructions);
            res_offsets[i] = res_offset;
        }
    }
};

}
