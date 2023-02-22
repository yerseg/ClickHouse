#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <Columns/IColumn.h>
#include <Common/typeid_cast.h>

#include <DataTypes/DataTypeAggregateFunction.h>
#include <Functions/IFunction.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ArrayJoinAction.h>
#include <Interpreters/FullSortingMergeJoin.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/TableJoin.h>

#include <Parsers/ASTWindowDefinition.h>

#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/CreateSetAndFilterOnTheFlyStep.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/CubeStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/TotalsHavingStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/QueryPlan/WindowStep.h>

#include <Storages/StorageMerge.h>

#include <stack>


namespace DB::QueryPlanOptimizations
{

static Poco::Logger * getLogger()
{
    static Poco::Logger & logger = Poco::Logger::get("QueryPlanOptimizations");
    return &logger;
}

using Positions = std::set<size_t>;
using Permutation = std::vector<size_t>;

ISourceStep * checkSupportedReadingStep(IQueryPlanStep * step)
{
    if (auto * reading = typeid_cast<ReadFromMergeTree *>(step))
    {
        /// Already read-in-order, skip.
        if (reading->getQueryInfo().input_order_info)
            return nullptr;

        const auto & sorting_key = reading->getStorageMetadata()->getSortingKey();
        if (sorting_key.column_names.empty())
            return nullptr;

        return reading;
    }

    if (auto * merge = typeid_cast<ReadFromMerge *>(step))
    {
        const auto & tables = merge->getSelectedTables();
        if (tables.empty())
            return nullptr;

        for (const auto & table : tables)
        {
            auto storage = std::get<StoragePtr>(table);
            const auto & sorting_key = storage->getInMemoryMetadataPtr()->getSortingKey();
            if (sorting_key.column_names.empty())
                return nullptr;
        }

        return merge;
    }

    return nullptr;
}

using StepStack = std::vector<IQueryPlanStep *>;

QueryPlan::Node * findReadingStep(QueryPlan::Node & node, StepStack & backward_path)
{
    IQueryPlanStep * step = node.step.get();
    if (auto * reading = checkSupportedReadingStep(step))
    {
        backward_path.push_back(node.step.get());
        return &node;
    }

    if (node.children.size() != 1)
        return nullptr;

    backward_path.push_back(node.step.get());

    if (typeid_cast<ExpressionStep *>(step) || typeid_cast<FilterStep *>(step) || typeid_cast<ArrayJoinStep *>(step))
        return findReadingStep(*node.children.front(), backward_path);

    if (auto * distinct = typeid_cast<DistinctStep *>(step); distinct && distinct->isPreliminary())
        return findReadingStep(*node.children.front(), backward_path);

    return nullptr;
}

void updateStepsDataStreams(StepStack & steps_to_update)
{
    /// update data stream's sorting properties for found transforms
    if (!steps_to_update.empty())
    {
        const DataStream * input_stream = &steps_to_update.back()->getOutputStream();
        chassert(dynamic_cast<ISourceStep *>(steps_to_update.back()));
        steps_to_update.pop_back();

        while (!steps_to_update.empty())
        {
            auto * transforming_step = dynamic_cast<ITransformingStep *>(steps_to_update.back());
            if (!transforming_step)
                break;

            transforming_step->updateInputStream(*input_stream);
            input_stream = &steps_to_update.back()->getOutputStream();
            steps_to_update.pop_back();
        }
    }
}

/// FixedColumns are columns which values become constants after filtering.
/// In a query "SELECT x, y, z FROM table WHERE x = 1 AND y = 'a' ORDER BY x, y, z"
/// Fixed columns are 'x' and 'y'.
using FixedColumns = std::unordered_set<const ActionsDAG::Node *>;

/// Right now we find only simple cases like 'and(..., and(..., and(column = value, ...), ...'
/// Injective functions are supported here. For a condition 'injectiveFunction(x) = 5' column 'x' is fixed.
void appendFixedColumnsFromFilterExpression(const ActionsDAG::Node & filter_expression, FixedColumns & fixed_columns)
{
    std::stack<const ActionsDAG::Node *> stack;
    stack.push(&filter_expression);

    while (!stack.empty())
    {
        const auto * node = stack.top();
        stack.pop();
        if (node->type == ActionsDAG::ActionType::FUNCTION)
        {
            const auto & name = node->function_base->getName();
            if (name == "and")
            {
                for (const auto * arg : node->children)
                    stack.push(arg);
            }
            else if (name == "equals")
            {
                const ActionsDAG::Node * maybe_fixed_column = nullptr;
                size_t num_constant_columns = 0;
                for (const auto & child : node->children)
                {
                    if (child->column)
                        ++num_constant_columns;
                    else
                        maybe_fixed_column = child;
                }

                if (maybe_fixed_column && num_constant_columns + 1 == node->children.size())
                {
                    LOG_TEST(getLogger(), "Added fixed column {} {}", maybe_fixed_column->result_name, static_cast<const void *>(maybe_fixed_column));
                    fixed_columns.insert(maybe_fixed_column);

                    /// Support injective functions chain.
                    const ActionsDAG::Node * maybe_injective = maybe_fixed_column;
                    while (maybe_injective->type == ActionsDAG::ActionType::FUNCTION
                        && maybe_injective->children.size() == 1
                        && maybe_injective->function_base->isInjective({}))
                    {
                        maybe_injective = maybe_injective->children.front();
                        fixed_columns.insert(maybe_injective);
                    }
                }
            }
        }
    }
}

void appendExpression(ActionsDAGPtr & dag, const ActionsDAGPtr & expression)
{
    if (dag)
        dag->mergeInplace(std::move(*expression->clone()));
    else
        dag = expression->clone();
}

/// This function builds a common DAG which is a gerge of DAGs from Filter and Expression steps chain.
/// Additionally, build a set of fixed columns.
void buildSortingDAG(QueryPlan::Node & node, ActionsDAGPtr & dag, FixedColumns & fixed_columns, size_t & limit)
{
    IQueryPlanStep * step = node.step.get();
    if (auto * reading = typeid_cast<ReadFromMergeTree *>(step))
    {
        if (const auto * prewhere_info = reading->getPrewhereInfo())
        {
            /// Should ignore limit if there is filtering.
            limit = 0;

            if (prewhere_info->prewhere_actions)
            {
                //std::cerr << "====== Adding prewhere " << std::endl;
                appendExpression(dag, prewhere_info->prewhere_actions);
                if (const auto * filter_expression = dag->tryFindInOutputs(prewhere_info->prewhere_column_name))
                    appendFixedColumnsFromFilterExpression(*filter_expression, fixed_columns);
            }
        }
        return;
    }

    if (node.children.size() != 1)
        return;

    buildSortingDAG(*node.children.front(), dag, fixed_columns, limit);

    if (auto * expression = typeid_cast<ExpressionStep *>(step))
    {
        const auto & actions = expression->getExpression();

        /// Should ignore limit because arrayJoin() can reduce the number of rows in case of empty array.
        if (actions->hasArrayJoin())
            limit = 0;

        appendExpression(dag, actions);
    }

    if (auto * filter = typeid_cast<FilterStep *>(step))
    {
        /// Should ignore limit if there is filtering.
        limit = 0;

        appendExpression(dag, filter->getExpression());
        if (const auto * filter_expression = dag->tryFindInOutputs(filter->getFilterColumnName()))
            appendFixedColumnsFromFilterExpression(*filter_expression, fixed_columns);
    }

    if (auto * array_join = typeid_cast<ArrayJoinStep *>(step))
    {
        /// Should ignore limit because ARRAY JOIN can reduce the number of rows in case of empty array.
        /// But in case of LEFT ARRAY JOIN the result number of rows is always bigger.
        if (!array_join->arrayJoin()->is_left)
            limit = 0;

        const auto & array_joined_columns = array_join->arrayJoin()->columns;

        /// Remove array joined columns from outputs.
        /// Types are changed after ARRAY JOIN, and we can't use this columns anyway.
        ActionsDAG::NodeRawConstPtrs outputs;
        outputs.reserve(dag->getOutputs().size());

        for (const auto & output : dag->getOutputs())
        {
            if (!array_joined_columns.contains(output->result_name))
                outputs.push_back(output);
        }
    }
}

/// Add more functions to fixed columns.
/// Functions result is fixed if all arguments are fixed or constants.
void enreachFixedColumns(const ActionsDAG & dag, FixedColumns & fixed_columns)
{
    struct Frame
    {
        const ActionsDAG::Node * node;
        size_t next_child = 0;
    };

    std::stack<Frame> stack;
    std::unordered_set<const ActionsDAG::Node *> visited;
    for (const auto & node : dag.getNodes())
    {
        if (visited.contains(&node))
            continue;

        stack.push({&node});
        visited.insert(&node);
        while (!stack.empty())
        {
            auto & frame = stack.top();
            for (; frame.next_child < frame.node->children.size(); ++frame.next_child)
                if (!visited.contains(frame.node->children[frame.next_child]))
                    break;

            if (frame.next_child < frame.node->children.size())
            {
                const auto * child = frame.node->children[frame.next_child];
                visited.insert(child);
                stack.push({child});
                ++frame.next_child;
            }
            else
            {
                /// Ignore constants here, will check them separately
                if (!frame.node->column)
                {
                    if (frame.node->type == ActionsDAG::ActionType::ALIAS)
                    {
                        if (fixed_columns.contains(frame.node->children.at(0)))
                            fixed_columns.insert(frame.node);
                    }
                    else if (frame.node->type == ActionsDAG::ActionType::FUNCTION)
                    {
                        if (frame.node->function_base->isDeterministicInScopeOfQuery())
                        {
                            //std::cerr << "*** enreachFixedColumns check " << frame.node->result_name << std::endl;
                            bool all_args_fixed_or_const = true;
                            for (const auto * child : frame.node->children)
                            {
                                if (!child->column && !fixed_columns.contains(child))
                                {
                                    //std::cerr << "*** enreachFixedColumns fail " << child->result_name <<  ' ' << static_cast<const void *>(child) << std::endl;
                                    all_args_fixed_or_const = false;
                                }
                            }

                            if (all_args_fixed_or_const)
                            {
                                //std::cerr << "*** enreachFixedColumns add " << frame.node->result_name << ' ' << static_cast<const void *>(frame.node) << std::endl;
                                fixed_columns.insert(frame.node);
                            }
                        }
                    }
                }

                stack.pop();
            }
        }
    }
}

InputOrderInfoPtr buildInputOrderInfo(
    const FixedColumns & fixed_columns,
    const ActionsDAGPtr & dag,
    const SortDescription & description,
    const ActionsDAG & sorting_key_dag,
    const Names & sorting_key_columns,
    size_t limit)
{
    //std::cerr << "------- buildInputOrderInfo " << std::endl;
    SortDescription order_key_prefix_descr;
    order_key_prefix_descr.reserve(description.size());

    MatchedTrees::Matches matches;
    FixedColumns fixed_key_columns;

    if (dag)
    {
        matches = matchTrees(sorting_key_dag, *dag);

        for (const auto & [node, match] : matches)
        {
            //std::cerr << "------- matching " << static_cast<const void *>(node) << " " << node->result_name
            //    << " to " << static_cast<const void *>(match.node) << " " << (match.node ? match.node->result_name : "") << std::endl;
            if (!match.monotonicity || match.monotonicity->strict)
            {
                if (match.node && fixed_columns.contains(node))
                    fixed_key_columns.insert(match.node);
            }
        }

        enreachFixedColumns(sorting_key_dag, fixed_key_columns);
    }

    /// This is a result direction we will read from MergeTree
    ///  1 - in order,
    /// -1 - in reverse order,
    ///  0 - usual read, don't apply optimization
    ///
    /// So far, 0 means any direction is possible. It is ok for constant prefix.
    int read_direction = 0;
    size_t next_description_column = 0;
    size_t next_sort_key = 0;

    while (next_description_column < description.size() && next_sort_key < sorting_key_columns.size())
    {
        const auto & sorting_key_column = sorting_key_columns[next_sort_key];
        const auto & sort_column_description = description[next_description_column];

        /// If required order depend on collation, it cannot be matched with primary key order.
        /// Because primary keys cannot have collations.
        if (sort_column_description.collator)
            break;

        /// Direction for current sort key.
        int current_direction = 0;
        bool strict_monotonic = true;

        const ActionsDAG::Node * sort_column_node = sorting_key_dag.tryFindInOutputs(sorting_key_column);
        /// This should not happen.
        if (!sort_column_node)
            break;

        if (!dag)
        {
            /// This is possible if there were no Expression or Filter steps in Plan.
            /// Example: SELECT * FROM tab ORDER BY a, b

            if (sort_column_node->type != ActionsDAG::ActionType::INPUT)
                break;

            if (sort_column_description.column_name != sorting_key_column)
                break;

            current_direction = sort_column_description.direction;


            //std::cerr << "====== (no dag) Found direct match" << std::endl;

            ++next_description_column;
            ++next_sort_key;
        }
        else
        {
            const ActionsDAG::Node * sort_node = dag->tryFindInOutputs(sort_column_description.column_name);
             /// It is possible when e.g. sort by array joined column.
            if (!sort_node)
                break;

            const auto & match = matches[sort_node];

            //std::cerr << "====== Finding match for " << sort_column_node->result_name << ' ' << static_cast<const void *>(sort_column_node) << std::endl;

            if (match.node && match.node == sort_column_node)
            {
                //std::cerr << "====== Found direct match" << std::endl;

                /// We try to find the match first even if column is fixed. In this case, potentially more keys will match.
                /// Example: 'table (x Int32, y Int32) ORDER BY x + 1, y + 1'
                ///          'SELECT x, y FROM table WHERE x = 42 ORDER BY x + 1, y + 1'
                /// Here, 'x + 1' would be a fixed point. But it is reasonable to read-in-order.

                current_direction = sort_column_description.direction;
                if (match.monotonicity)
                {
                    current_direction *= match.monotonicity->direction;
                    strict_monotonic = match.monotonicity->strict;
                }

                ++next_description_column;
                ++next_sort_key;
            }
            else if (fixed_key_columns.contains(sort_column_node))
            {
                //std::cerr << "+++++++++ Found fixed key by match" << std::endl;
                ++next_sort_key;
            }
            else
            {

                //std::cerr << "====== Check for fixed const : " << bool(sort_node->column) << " fixed : " << fixed_columns.contains(sort_node) << std::endl;
                bool is_fixed_column = sort_node->column || fixed_columns.contains(sort_node);
                if (!is_fixed_column)
                    break;

                order_key_prefix_descr.push_back(sort_column_description);
                ++next_description_column;
            }
        }

        /// read_direction == 0 means we can choose any global direction.
        /// current_direction == 0 means current key if fixed and any direction is possible for it.
        if (current_direction && read_direction && current_direction != read_direction)
            break;

        if (read_direction == 0)
            read_direction = current_direction;

        if (current_direction)
            order_key_prefix_descr.push_back(sort_column_description);

        if (current_direction && !strict_monotonic)
            break;
    }

    if (read_direction == 0 || order_key_prefix_descr.empty())
        return nullptr;

    return std::make_shared<InputOrderInfo>(order_key_prefix_descr, next_sort_key, read_direction, limit);
}

/// Sort description for step that requires sorting (aggregation or sorting JOIN).
/// Note: We really do need three different sort descriptions here.
///
/// For example, aggregation query:
///
///   create table tab (a Int32, b Int32, c Int32, d Int32) engine = MergeTree order by (a, b, c);
///   select a, any(b), c, d from tab where b = 1 group by a, c, d order by c, d;
///
/// We would like to have:
/// (a, b, c) - a sort description for reading from table (it's into input_order)
/// (a, c) - a sort description for merging (an input of AggregatingInOrderTransform is sorted by this GROUP BY keys)
/// (a, c, d) - a target sort description (GROUP BY sort description, an input of FinishAggregatingInOrderTransform is sorted by all GROUP BY keys)
///
/// Sort description from input_order is not actually used. ReadFromMergeTree reads only PK prefix size.
/// We should remove it later.
struct StepInputOrder
{
    InputOrderInfoPtr input_order;
    SortDescription sort_description_for_merging;
    SortDescription target_sort_description;

    /// Map indices from target_sort_description original positions
    std::vector<Positions> permutation;
};

StepInputOrder buildInputOrderInfo(
    const FixedColumns & fixed_columns,
    const ActionsDAGPtr & dag,
    const Names & key_names,
    const ActionsDAG & sorting_key_dag,
    const Names & sorting_key_columns,
    int read_direction = 0)
{
    MatchedTrees::Matches matches;
    FixedColumns fixed_key_columns;

    /// For every column in PK find any match from GROUP BY key.
    using ReverseMatches = std::unordered_map<const ActionsDAG::Node *, MatchedTrees::Matches::const_iterator>;
    ReverseMatches reverse_matches;

    /// Map from key_name to its positions in key_names.
    std::unordered_map<std::string_view, Positions> not_matched_keys;
    for (size_t i = 0; i < key_names.size(); ++i)
        not_matched_keys[key_names[i]].insert(i);

    if (dag)
    {
        matches = matchTrees(sorting_key_dag, *dag);
        for (const auto & [node, match] : matches)
        {
            if (!match.monotonicity || match.monotonicity->strict)
            {
                if (match.node && fixed_columns.contains(node))
                    fixed_key_columns.insert(match.node);
            }
        }

        enreachFixedColumns(sorting_key_dag, fixed_key_columns);

        for (auto it = matches.cbegin(); it != matches.cend(); ++it)
        {
            const MatchedTrees::Match * match = &it->second;
            if (match->node)
            {
                auto [jt, inserted] = reverse_matches.emplace(match->node, it);
                if (!inserted)
                {
                    /// Find the best match for PK node.
                    /// Direct match > strict monotonic > monotonic.
                    const MatchedTrees::Match * prev_match = &jt->second->second;
                    bool is_better = prev_match->monotonicity && !match->monotonicity;

                    String prev_name = jt->second->first->result_name;
                    String new_name = it->first->result_name;
                    if (!is_better)
                    {
                        bool both_monotionic = prev_match->monotonicity && match->monotonicity;
                        is_better = both_monotionic && match->monotonicity->strict && !prev_match->monotonicity->strict;
                        bool new_name_matches = !not_matched_keys.contains(prev_name) && not_matched_keys.contains(new_name);
                        is_better = is_better || new_name_matches;
                    }

                    if (is_better)
                        jt->second = it;
                }
            }
        }
    }

    size_t next_sort_key = 0;

    /// Maps elements from target_sort_description (with corresponding indices) to original positions in key_names.
    std::vector<Positions> permutation;
    permutation.reserve(not_matched_keys.size());

    SortDescription target_sort_description;
    target_sort_description.reserve(not_matched_keys.size());

    SortDescription order_key_prefix_descr;
    order_key_prefix_descr.reserve(sorting_key_columns.size());

    while (!not_matched_keys.empty() && next_sort_key < sorting_key_columns.size())
    {
        const auto & sorting_key_column = sorting_key_columns[next_sort_key];

        /// This is a result direction we will read from MergeTree
        ///  1 - in order,
        /// -1 - in reverse order,
        ///  0 - usual read, don't apply optimization
        ///
        /// So far, 0 means any direction is possible. It is ok for constant prefix.
        int current_direction = 0;
        bool strict_monotonic = true;
        typename decltype(not_matched_keys)::iterator group_by_key_it;

        const ActionsDAG::Node * sort_column_node = sorting_key_dag.tryFindInOutputs(sorting_key_column);
        /// This should not happen.
        if (!sort_column_node)
        {
            LOG_WARNING(getLogger(), "No sort_column_node for '{}'", sorting_key_column);
            break;
        }

        if (!dag)
        {
            /// This is possible if there were no Expression or Filter steps in Plan.
            /// Example: SELECT * FROM tab ORDER BY a, b

            if (sort_column_node->type != ActionsDAG::ActionType::INPUT)
                break;

            group_by_key_it = not_matched_keys.find(sorting_key_column);
            if (group_by_key_it == not_matched_keys.end())
                break;

            current_direction = 1;

            LOG_TEST(getLogger(), "Found direct match with (no dag)");
            ++next_sort_key;
        }
        else
        {
            const MatchedTrees::Match * match = nullptr;
            const ActionsDAG::Node * group_by_key_node = nullptr;
            if (const auto match_it = reverse_matches.find(sort_column_node); match_it != reverse_matches.end())
            {
                group_by_key_node = match_it->second->first;
                match = &match_it->second->second;
            }

            LOG_TEST(getLogger(), "Finding match for {} {}", sort_column_node->result_name, static_cast<const void *>(sort_column_node));

            if (match && match->node)
                group_by_key_it = not_matched_keys.find(group_by_key_node->result_name);

            if (match && match->node && group_by_key_it != not_matched_keys.end())
            {
                LOG_TEST(getLogger(), "Found direct match");

                current_direction = 1;
                if (match->monotonicity)
                {
                    current_direction *= match->monotonicity->direction;
                    strict_monotonic = match->monotonicity->strict;
                }

                ++next_sort_key;
            }
            else if (fixed_key_columns.contains(sort_column_node))
            {
                LOG_TEST(getLogger(), "Found fixed key by match");
                ++next_sort_key;
            }
            else
                break;
        }

        /// read_direction == 0 means we can choose any global direction.
        /// current_direction == 0 means current key if fixed and any direction is possible for it.
        if (current_direction && read_direction && current_direction != read_direction)
            break;

        if (read_direction == 0 && current_direction != 0)
            read_direction = current_direction;

        if (current_direction)
        {
            /// Aggregation in order will always read in table order.
            /// Here, current_direction is a direction which will be applied to every key.
            /// Example:
            ///   CREATE TABLE t (x, y, z) ENGINE = MergeTree ORDER BY (x, y)
            ///   SELECT ... FROM t GROUP BY negate(y), negate(x), z
            /// Here, current_direction will be -1 cause negate() is negative montonic,
            /// Prefix sort description for reading will be (negate(y) DESC, negate(x) DESC),
            /// Sort description for GROUP BY will be (negate(y) DESC, negate(x) DESC, z).
            LOG_TEST(getLogger(), "Adding {} to sort_description", std::string(group_by_key_it->first));
            target_sort_description.emplace_back(SortColumnDescription(std::string(group_by_key_it->first), current_direction));
            permutation.emplace_back(group_by_key_it->second);

            order_key_prefix_descr.emplace_back(SortColumnDescription(std::string(group_by_key_it->first), current_direction));
            not_matched_keys.erase(group_by_key_it);
        }
        else
        {
            /// If column is fixed, will read it in table order as well.
            LOG_TEST(getLogger(), "Adding {} to sort_description", sorting_key_column);
            order_key_prefix_descr.emplace_back(SortColumnDescription(sorting_key_column, 1));
        }

        if (current_direction && !strict_monotonic)
            break;
    }

    if (read_direction == 0 || target_sort_description.empty())
        return {};

    SortDescription sort_description_for_merging = target_sort_description;

    for (const auto & [key, positions] : not_matched_keys)
    {
        target_sort_description.emplace_back(SortColumnDescription(std::string(key)));
        permutation.emplace_back(positions);
    }

    auto input_order = std::make_shared<InputOrderInfo>(order_key_prefix_descr, next_sort_key, /* read_direction */ 1, /* limit */ 0);
    return { std::move(input_order), std::move(sort_description_for_merging), std::move(target_sort_description), std::move(permutation) };
}

InputOrderInfoPtr buildInputOrderInfo(
    const ReadFromMergeTree * reading,
    const FixedColumns & fixed_columns,
    const ActionsDAGPtr & dag,
    const SortDescription & description,
    size_t limit)
{
    const auto & sorting_key = reading->getStorageMetadata()->getSortingKey();
    const auto & sorting_key_columns = sorting_key.column_names;

    return buildInputOrderInfo(
        fixed_columns,
        dag, description,
        sorting_key.expression->getActionsDAG(), sorting_key_columns,
        limit);
}

InputOrderInfoPtr buildInputOrderInfo(
    ReadFromMerge * merge,
    const FixedColumns & fixed_columns,
    const ActionsDAGPtr & dag,
    const SortDescription & description,
    size_t limit)
{
    const auto & tables = merge->getSelectedTables();

    InputOrderInfoPtr order_info;
    for (const auto & table : tables)
    {
        auto storage = std::get<StoragePtr>(table);
        const auto & sorting_key = storage->getInMemoryMetadataPtr()->getSortingKey();
        const auto & sorting_key_columns = sorting_key.column_names;

        if (sorting_key_columns.empty())
            return nullptr;

        auto table_order_info = buildInputOrderInfo(
            fixed_columns,
            dag, description,
            sorting_key.expression->getActionsDAG(), sorting_key_columns,
            limit);

        if (!table_order_info)
            return nullptr;

        if (!order_info)
            order_info = table_order_info;
        else if (*order_info != *table_order_info)
            return nullptr;
    }

    return order_info;
}

StepInputOrder buildInputOrderInfo(
    ReadFromMergeTree * reading,
    const FixedColumns & fixed_columns,
    const ActionsDAGPtr & dag,
    const Names & keys)
{
    const auto & sorting_key = reading->getStorageMetadata()->getSortingKey();
    const auto & sorting_key_columns = sorting_key.column_names;

    return buildInputOrderInfo(
        fixed_columns,
        dag, keys,
        sorting_key.expression->getActionsDAG(),
        sorting_key_columns);
}

StepInputOrder buildInputOrderInfo(
    ReadFromMerge * merge,
    const FixedColumns & fixed_columns,
    const ActionsDAGPtr & dag,
    const Names & group_by_keys)
{
    const auto & tables = merge->getSelectedTables();

    StepInputOrder order_info;
    for (const auto & table : tables)
    {
        auto storage = std::get<StoragePtr>(table);
        const auto & sorting_key = storage->getInMemoryMetadataPtr()->getSortingKey();
        const auto & sorting_key_columns = sorting_key.column_names;

        if (sorting_key_columns.empty())
            return {};

        auto table_order_info = buildInputOrderInfo(
            fixed_columns,
            dag, group_by_keys,
            sorting_key.expression->getActionsDAG(), sorting_key_columns);

        if (!table_order_info.input_order)
            return {};

        if (!order_info.input_order)
            order_info = table_order_info;
        else if (*order_info.input_order != *table_order_info.input_order)
            return {};
    }

    return order_info;
}

InputOrderInfoPtr buildInputOrderInfo(const SortingStep & sorting, QueryPlan::Node & node, StepStack & backward_path)
{
    QueryPlan::Node * reading_node = findReadingStep(node, backward_path);
    if (!reading_node)
        return nullptr;

    const auto & description = sorting.getSortDescription();
    size_t limit = sorting.getLimit();

    ActionsDAGPtr dag;
    FixedColumns fixed_columns;
    buildSortingDAG(node, dag, fixed_columns, limit);

    if (dag && !fixed_columns.empty())
        enreachFixedColumns(*dag, fixed_columns);

    if (auto * reading = typeid_cast<ReadFromMergeTree *>(reading_node->step.get()))
    {
        auto order_info = buildInputOrderInfo(
            reading,
            fixed_columns,
            dag, description,
            limit);

        if (order_info)
        {
            bool can_read = reading->requestReadingInOrder(order_info->used_prefix_of_sorting_key_size, order_info->direction, order_info->limit);
            if (!can_read)
                return nullptr;
        }

        return order_info;
    }
    else if (auto * merge = typeid_cast<ReadFromMerge *>(reading_node->step.get()))
    {
        auto order_info = buildInputOrderInfo(
            merge,
            fixed_columns,
            dag, description,
            limit);

        if (order_info)
        {
            bool can_read = merge->requestReadingInOrder(order_info);
            if (!can_read)
                return nullptr;
        }

        return order_info;
    }

    return nullptr;
}

StepInputOrder buildInputOrderInfo(const Names & keys, QueryPlan::Node & node, QueryPlan::Node * reading_node)
{
    if (!reading_node)
        return {};

    size_t limit = 0;

    ActionsDAGPtr dag;
    FixedColumns fixed_columns;
    buildSortingDAG(node, dag, fixed_columns, limit);

    if (dag && !fixed_columns.empty())
        enreachFixedColumns(*dag, fixed_columns);

    if (auto * reading = typeid_cast<ReadFromMergeTree *>(reading_node->step.get()))
    {
        auto order_info = buildInputOrderInfo(
            reading,
            fixed_columns,
            dag, keys);

        if (order_info.input_order)
        {
            bool can_read = reading->requestReadingInOrder(
                order_info.input_order->used_prefix_of_sorting_key_size,
                order_info.input_order->direction,
                order_info.input_order->limit);
            if (!can_read)
                return {};
        }

        return order_info;
    }
    else if (auto * merge = typeid_cast<ReadFromMerge *>(reading_node->step.get()))
    {
        auto order_info = buildInputOrderInfo(merge, fixed_columns, dag, keys);
        if (order_info.input_order)
        {
            bool can_read = merge->requestReadingInOrder(order_info.input_order);
            if (!can_read)
                return {};
        }
        return order_info;
    }

    return {};
}

void requestInputOrderInfo(const InputOrderInfoPtr & input_order_info, QueryPlanStepPtr & reading_step)
{
    if (auto * reading = typeid_cast<ReadFromMergeTree *>(reading_step.get()))
        reading->requestReadingInOrder(
            input_order_info->used_prefix_of_sorting_key_size,
            input_order_info->direction,
            input_order_info->limit);
    else if (auto * merge = typeid_cast<ReadFromMerge *>(reading_step.get()))
        merge->requestReadingInOrder(input_order_info);
}

void optimizeReadInOrder(QueryPlan::Node & node, QueryPlan::Nodes & nodes)
{
    if (node.children.size() != 1)
        return;

    auto * sorting = typeid_cast<SortingStep *>(node.step.get());
    if (!sorting)
        return;

    LOG_DEBUG(getLogger(), "optimizeReadInOrder found sorting");

    if (sorting->getType() != SortingStep::Type::Full)
        return;

    StepStack steps_to_update;
    if (typeid_cast<UnionStep *>(node.children.front()->step.get()))
    {
        auto & union_node = node.children.front();

        std::vector<InputOrderInfoPtr> infos;
        const SortDescription * max_sort_descr = nullptr;
        infos.reserve(node.children.size());
        for (auto * child : union_node->children)
        {
            infos.push_back(buildInputOrderInfo(*sorting, *child, steps_to_update));

            if (infos.back() && (!max_sort_descr || max_sort_descr->size() < infos.back()->sort_description_for_merging.size()))
                max_sort_descr = &infos.back()->sort_description_for_merging;
        }

        if (!max_sort_descr || max_sort_descr->empty())
            return;

        for (size_t i = 0; i < infos.size(); ++i)
        {
            const auto & info = infos[i];
            auto & child = union_node->children[i];

            QueryPlanStepPtr additional_sorting;

            if (!info)
            {
                auto limit = sorting->getLimit();
                /// If we have limit, it's better to sort up to full description and apply limit.
                /// We cannot sort up to partial read-in-order description with limit cause result set can be wrong.
                const auto & descr = limit ? sorting->getSortDescription() : *max_sort_descr;
                additional_sorting = std::make_unique<SortingStep>(
                    child->step->getOutputStream(),
                    descr,
                    limit, /// TODO: support limit with ties
                    sorting->getSettings(),
                    false);
            }
            else if (info->sort_description_for_merging.size() < max_sort_descr->size())
            {
                additional_sorting = std::make_unique<SortingStep>(
                    child->step->getOutputStream(),
                    info->sort_description_for_merging,
                    *max_sort_descr,
                    sorting->getSettings().max_block_size,
                    0); /// TODO: support limit with ties
            }

            if (additional_sorting)
            {
                auto & sort_node = nodes.emplace_back();
                sort_node.step = std::move(additional_sorting);
                sort_node.children.push_back(child);
                child = &sort_node;
            }
        }

        sorting->convertToFinishSorting(*max_sort_descr);
    }
    else if (auto order_info = buildInputOrderInfo(*sorting, *node.children.front(), steps_to_update))
    {
        sorting->convertToFinishSorting(order_info->sort_description_for_merging);
        /// update data stream's sorting properties
        updateStepsDataStreams(steps_to_update);
    }
}

void optimizeAggregationInOrder(QueryPlan::Node & node, QueryPlan::Nodes &)
{
    if (node.children.size() != 1)
        return;

    auto * aggregating = typeid_cast<AggregatingStep *>(node.step.get());
    if (!aggregating)
        return;

    if ((aggregating->inOrder() && !aggregating->explicitSortingRequired()) || aggregating->isGroupingSets())
        return;

    auto * child_node = node.children.front();

    StepStack steps_to_update;

    /// TODO: maybe add support for UNION later.
    QueryPlan::Node * reading_node = findReadingStep(node, steps_to_update);

    auto order_info = buildInputOrderInfo(aggregating->getParams().keys, *child_node, reading_node);
    if (order_info.input_order)
    {
        requestInputOrderInfo(order_info.input_order, reading_node->step);

        aggregating->applyOrder(std::move(order_info.sort_description_for_merging), std::move(order_info.target_sort_description));
        /// update data stream's sorting properties
        updateStepsDataStreams(steps_to_update);
    }
}

static Permutation flattenPositions(const std::vector<Positions> & positions)
{
    Permutation res;
    res.reserve(positions.size());
    for (const auto & pos : positions)
        res.insert(res.end(), pos.begin(), pos.end());
    return res;
}

static size_t findPrefixMatchLength(const std::vector<Positions> & positions, const Permutation & flattened)
{
    size_t flatten_idx = 0;
    for (size_t i = 0; i < positions.size(); ++i)
    {
        for (size_t e : positions[i])
        {
            if (e != flattened[flatten_idx])
                return i;
            ++flatten_idx;
        }
    }
    return positions.size();
}

/// Choose order info that use longer prefix of sorting key.
/// Returns true if lhs is better than rhs.
static bool compareOrderInfos(const InputOrderInfoPtr & lhs, const InputOrderInfoPtr & rhs)
{
    size_t lhs_size = lhs ? lhs->used_prefix_of_sorting_key_size : 0;
    size_t rhs_size = rhs ? rhs->used_prefix_of_sorting_key_size : 0;
    return lhs_size >= rhs_size;
}

/// Cut order info to use only prefix of specified size
static void truncateToPrefixSize(StepInputOrder & order_info, size_t prefix_size)
{
    if (!order_info.input_order)
        return;

    chassert(order_info.target_sort_description.size() >= order_info.sort_description_for_merging.size());

    if (order_info.target_sort_description.size() <= prefix_size)
        return;

    order_info.target_sort_description.resize(prefix_size);
    order_info.sort_description_for_merging.resize(prefix_size);

    const auto & last_column_name = order_info.sort_description_for_merging[prefix_size - 1].column_name;

    chassert(order_info.input_order->sort_description_for_merging.size() == order_info.input_order->used_prefix_of_sorting_key_size);

    auto input_order = std::make_shared<InputOrderInfo>(*order_info.input_order);
    for (size_t i = 0; i < input_order->sort_description_for_merging.size(); ++i)
    {
        if (input_order->sort_description_for_merging[i].column_name == last_column_name)
        {
            input_order->used_prefix_of_sorting_key_size = i + 1;
            input_order->sort_description_for_merging.resize(input_order->used_prefix_of_sorting_key_size);
            break;
        }
    }
    order_info.input_order = std::move(input_order);
}

/// Choose order info that use longer prefix of sorting key and cut second one to use common prefix.
/// Returns permutation that should be applied to keys.
static Permutation findCommonOrderInfo(StepInputOrder & left_order_info, StepInputOrder & right_order_info)
{
    if (!left_order_info.input_order && !right_order_info.input_order)
    {
        LOG_TRACE(getLogger(), "Cannot read anything in order for join");
        return {};
    }

    if (!right_order_info.input_order)
    {
        LOG_TRACE(getLogger(), "Can read left stream in order for join");
        return flattenPositions(left_order_info.permutation);
    }

    if (!left_order_info.input_order)
    {
        LOG_TRACE(getLogger(), "Can read right stream in order for join");
        return flattenPositions(right_order_info.permutation);
    }

    LOG_TRACE(getLogger(), "Can read both streams in order for join");

    bool left_is_better = compareOrderInfos(left_order_info.input_order, right_order_info.input_order);
    auto & lhs = left_is_better ? left_order_info : right_order_info;
    auto & rhs = left_is_better ? right_order_info : left_order_info;

    Permutation result_permutation = flattenPositions(lhs.permutation);
    size_t prefix_size = findPrefixMatchLength(rhs.permutation, result_permutation);
    truncateToPrefixSize(rhs, prefix_size);
    return result_permutation;
}

static bool optimizeJoinInOrder(QueryPlan::Node & node, const std::shared_ptr<FullSortingMergeJoin> & join_ptr)
{
    const auto & key_names_left = join_ptr->getKeyNames(JoinTableSide::Left);
    auto * left_child_node = node.children[0];

    StepStack steps_to_update_left;

    QueryPlan::Node * left_reading_node = findReadingStep(*left_child_node->children.front(), steps_to_update_left);
    auto left_order_info = buildInputOrderInfo(key_names_left, *left_child_node, left_reading_node);

    const auto & key_names_right = join_ptr->getKeyNames(JoinTableSide::Right);
    auto * right_child_node = node.children[1];

    StepStack steps_to_update_right;
    QueryPlan::Node * right_reading_node = findReadingStep(*right_child_node->children.front(), steps_to_update_right);
    auto right_order_info = buildInputOrderInfo(key_names_right, *right_child_node, right_reading_node);

    auto keys_permuation = findCommonOrderInfo(left_order_info, right_order_info);
    if (keys_permuation.empty())
        return false;

    LOG_TRACE(getLogger(), "Applying permutation [{}] for join keys", fmt::join(keys_permuation, ", "));

    join_ptr->permuteKeys(keys_permuation);

    if (left_order_info.input_order)
    {
        join_ptr->setPrefixSortDesctiption(left_order_info.sort_description_for_merging, JoinTableSide::Left);
        requestInputOrderInfo(left_order_info.input_order, left_reading_node->step);
        updateStepsDataStreams(steps_to_update_left);
    }

    if (right_order_info.input_order)
    {
        join_ptr->setPrefixSortDesctiption(right_order_info.sort_description_for_merging, JoinTableSide::Right);
        requestInputOrderInfo(right_order_info.input_order, right_reading_node->step);
        updateStepsDataStreams(steps_to_update_right);
    }
    return true;
}

void applyOrderForJoin(QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & optimization_settings)
{
    if (node.children.size() != 2 || node.children[0]->children.size() != 1 || node.children[1]->children.size() != 1)
        return;

    auto * join_step = typeid_cast<JoinStep *>(node.step.get());
    if (!join_step)
        return;

    auto join_ptr = std::dynamic_pointer_cast<FullSortingMergeJoin>(join_step->getJoin());
    if (!join_ptr)
        return;

    bool is_read_in_order_optimized = false;
    if (optimization_settings.read_in_order && optimization_settings.join_in_order)
        is_read_in_order_optimized = optimizeJoinInOrder(node, join_ptr);

    auto insert_pre_step = [&nodes, &node](size_t idx, auto step)
    {
        auto & sort_node = nodes.emplace_back();
        sort_node.step = std::move(step);
        sort_node.children.push_back(node.children[idx]);
        node.children[idx] = &sort_node;
    };

    auto join_kind = join_ptr->getTableJoin().kind();
    bool kind_allows_filtering = isInner(join_kind) || isLeft(join_kind) || isRight(join_kind);

    auto has_non_const = [](const Block & block, const auto & keys)
    {
        for (const auto & key : keys)
        {
            const auto & column = block.getByName(key).column;
            if (column && !isColumnConst(*column))
                return true;
        }
        return false;
    };

    const auto & left_stream = node.children[0]->step->getOutputStream();
    const auto & right_stream = node.children[1]->step->getOutputStream();

    /// This optimization relies on the sorting that should buffer the whole stream before emitting any rows.
    /// It doesn't hold such a guarantee for streams with const keys.
    /// Note: it's also doesn't work with the read-in-order optimization.
    /// No checks here because read in order is not applied if we have `CreateSetAndFilterOnTheFlyStep` in the pipeline between the reading and sorting steps.
    bool has_non_const_keys = has_non_const(left_stream.header, join_ptr->getKeyNames(JoinTableSide::Left))
        && has_non_const(right_stream.header, join_ptr->getKeyNames(JoinTableSide::Right));

    size_t max_rows_in_filter_set = optimization_settings.max_rows_in_set_to_optimize_join;
    if (!is_read_in_order_optimized && max_rows_in_filter_set > 0 && kind_allows_filtering && has_non_const_keys)
    {
        auto crosswise_connection = CreateSetAndFilterOnTheFlyStep::createCrossConnection();
        auto add_create_set = [&](const DataStream & data_stream, const Names & key_names, JoinTableSide join_pos)
        {
            auto creating_set_step = std::make_unique<CreateSetAndFilterOnTheFlyStep>(
                data_stream, key_names, max_rows_in_filter_set, crosswise_connection, join_pos);
            creating_set_step->setStepDescription(fmt::format("Create set and filter {} joined stream", join_pos));

            auto * step_raw_ptr = creating_set_step.get();
            insert_pre_step(join_pos == JoinTableSide::Left ? 0 : 1, std::move(creating_set_step));
            return step_raw_ptr;
        };

        auto * left_set = add_create_set(left_stream, join_ptr->getKeyNames(JoinTableSide::Left), JoinTableSide::Left);
        auto * right_set = add_create_set(right_stream, join_ptr->getKeyNames(JoinTableSide::Right), JoinTableSide::Right);
        if (isInnerOrLeft(join_kind))
            right_set->setFiltering(left_set->getSet());
        if (isInnerOrRight(join_kind))
            left_set->setFiltering(right_set->getSet());
    }

    for (size_t i = 0; i < 2; ++i)
    {
        insert_pre_step(i, join_step->createSorting(JoinTableSide(i)));
    }
}

/// This optimisation is obsolete and will be removed.
/// optimizeReadInOrder covers it.
size_t tryReuseStorageOrderingForWindowFunctions(QueryPlan::Node * parent_node, QueryPlan::Nodes & /*nodes*/)
{
    /// Find the following sequence of steps, add InputOrderInfo and apply prefix sort description to
    /// SortingStep:
    /// WindowStep <- SortingStep <- [Expression] <- ReadFromMergeTree

    auto * window_node = parent_node;
    auto * window = typeid_cast<WindowStep *>(window_node->step.get());
    if (!window)
        return 0;
    if (window_node->children.size() != 1)
        return 0;

    auto * sorting_node = window_node->children.front();
    auto * sorting = typeid_cast<SortingStep *>(sorting_node->step.get());
    if (!sorting)
        return 0;
    if (sorting_node->children.size() != 1)
        return 0;

    auto * possible_read_from_merge_tree_node = sorting_node->children.front();

    if (typeid_cast<ExpressionStep *>(possible_read_from_merge_tree_node->step.get()))
    {
        if (possible_read_from_merge_tree_node->children.size() != 1)
            return 0;

        possible_read_from_merge_tree_node = possible_read_from_merge_tree_node->children.front();
    }

    auto * read_from_merge_tree = typeid_cast<ReadFromMergeTree *>(possible_read_from_merge_tree_node->step.get());
    if (!read_from_merge_tree)
    {
        return 0;
    }

    auto context = read_from_merge_tree->getContext();
    const auto & settings = context->getSettings();
    if (!settings.optimize_read_in_window_order || (settings.optimize_read_in_order && settings.query_plan_read_in_order) || context->getSettingsRef().allow_experimental_analyzer)
    {
        return 0;
    }

    const auto & query_info = read_from_merge_tree->getQueryInfo();
    const auto * select_query = query_info.query->as<ASTSelectQuery>();

    /// TODO: Analyzer syntax analyzer result
    if (!query_info.syntax_analyzer_result)
        return 0;

    ManyExpressionActions order_by_elements_actions;
    const auto & window_desc = window->getWindowDescription();

    for (const auto & actions_dag : window_desc.partition_by_actions)
    {
        order_by_elements_actions.emplace_back(
            std::make_shared<ExpressionActions>(actions_dag, ExpressionActionsSettings::fromContext(context, CompileExpressions::yes)));
    }

    for (const auto & actions_dag : window_desc.order_by_actions)
    {
        order_by_elements_actions.emplace_back(
            std::make_shared<ExpressionActions>(actions_dag, ExpressionActionsSettings::fromContext(context, CompileExpressions::yes)));
    }

    auto order_optimizer = std::make_shared<ReadInOrderOptimizer>(
            *select_query,
            order_by_elements_actions,
            window->getWindowDescription().full_sort_description,
            query_info.syntax_analyzer_result);

    /// If we don't have filtration, we can pushdown limit to reading stage for optimizations.
    UInt64 limit = (select_query->hasFiltration() || select_query->groupBy()) ? 0 : InterpreterSelectQuery::getLimitForSorting(*select_query, context);

    auto order_info = order_optimizer->getInputOrder(
            query_info.projection ? query_info.projection->desc->metadata : read_from_merge_tree->getStorageMetadata(),
            context,
            limit);

    if (order_info)
    {
        bool can_read = read_from_merge_tree->requestReadingInOrder(order_info->used_prefix_of_sorting_key_size, order_info->direction, order_info->limit);
        if (!can_read)
            return 0;
        sorting->convertToFinishSorting(order_info->sort_description_for_merging);
    }

    return 0;
}

}
