#include <Backups/BackupCoordinationStageSync.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <base/chrono_io.h>
#include <Common/Exception.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/ZooKeeperWithFaultInjection.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int FAILED_TO_SYNC_BACKUP_OR_RESTORE;
}


BackupCoordinationStageSync::BackupCoordinationStageSync(
    const String & root_zookeeper_path_, CoordinationSettings settings_, zkutil::GetZooKeeper get_zookeeper_, Poco::Logger * log_)
    : zookeeper_path(root_zookeeper_path_ + "/stage")
    , get_zookeeper(get_zookeeper_)
    , log(log_)
    , global_zookeeper_retries_info(
        "BackupCoordinationStageSync",
        log,
        settings_.keeper_max_retries,
        settings_.keeper_retry_initial_backoff_ms,
        settings_.keeper_retry_max_backoff_ms)
{
    createRootNodes();
}

void BackupCoordinationStageSync::createRootNodes()
{
    auto zookeeper_retries_info = global_zookeeper_retries_info;
    ZooKeeperRetriesControl retries_ctl("createRootNodes", zookeeper_retries_info);
    retries_ctl.retryLoop(
        [&]()
        {
            auto zookeeper = get_zookeeper();
            zookeeper->createAncestors(zookeeper_path);
            zookeeper->createIfNotExists(zookeeper_path, "");
        });
}

void BackupCoordinationStageSync::set(const String & current_host, const String & new_stage, const String & message)
{
    auto zookeeper_retries_info = global_zookeeper_retries_info;
    ZooKeeperRetriesControl retries_ctl("set", zookeeper_retries_info);
    retries_ctl.retryLoop(
        [&]()
        {
            auto zookeeper = get_zookeeper();

            /// Make an ephemeral node so the initiator can track if the current host is still working.
            String alive_node_path = zookeeper_path + "/alive|" + current_host;
            auto code = zookeeper->tryCreate(alive_node_path, "", zkutil::CreateMode::Ephemeral);
            if (code != Coordination::Error::ZOK && code != Coordination::Error::ZNODEEXISTS)
                throw zkutil::KeeperException(code, alive_node_path);

            zookeeper->createIfNotExists(zookeeper_path + "/started|" + current_host, "");
            zookeeper->createIfNotExists(zookeeper_path + "/current|" + current_host + "|" + new_stage, message);
        });
}

void BackupCoordinationStageSync::setError(const String & current_host, const Exception & exception)
{
    auto zookeeper_retries_info = global_zookeeper_retries_info;
    ZooKeeperRetriesControl retries_ctl("setError", zookeeper_retries_info);
    retries_ctl.retryLoop(
        [&]()
        {
            auto zookeeper = get_zookeeper();
            WriteBufferFromOwnString buf;
            writeStringBinary(current_host, buf);
            writeException(exception, buf, true);
            zookeeper->createIfNotExists(zookeeper_path + "/error", buf.str());
        });
}

Strings BackupCoordinationStageSync::wait(const Strings & all_hosts, const String & stage_to_wait)
{
    return waitImpl(all_hosts, stage_to_wait, {});
}

Strings BackupCoordinationStageSync::waitFor(const Strings & all_hosts, const String & stage_to_wait, std::chrono::milliseconds timeout)
{
    return waitImpl(all_hosts, stage_to_wait, timeout);
}

namespace
{
    struct UnreadyHostState
    {
        bool started = false;
        bool alive = false;
    };
}

struct BackupCoordinationStageSync::State
{
    Strings results;
    std::map<String, UnreadyHostState> unready_hosts;
    std::optional<std::pair<String, Exception>> error;
    std::optional<String> host_terminated;
};

BackupCoordinationStageSync::State BackupCoordinationStageSync::readCurrentState(
    zkutil::ZooKeeperPtr zookeeper, const Strings & zk_nodes, const Strings & all_hosts, const String & stage_to_wait) const
{
    std::unordered_set<std::string_view> zk_nodes_set{zk_nodes.begin(), zk_nodes.end()};

    State state;
    if (zk_nodes_set.contains("error"))
    {
        ReadBufferFromOwnString buf{zookeeper->get(zookeeper_path + "/error")};
        String host;
        readStringBinary(host, buf);
        state.error = std::make_pair(host, readException(buf, fmt::format("Got error from {}", host)));
        return state;
    }

    for (const auto & host : all_hosts)
    {
        if (!zk_nodes_set.contains("current|" + host + "|" + stage_to_wait))
        {
            UnreadyHostState unready_host_state;
            const String started_node_name = "started|" + host;
            const String alive_node_name = "alive|" + host;
            const String alive_node_path = zookeeper_path + "/" + alive_node_name;
            unready_host_state.started = zk_nodes_set.contains(started_node_name);

            /// Because we do retries everywhere we can't fully rely on ephemeral nodes anymore.
            /// Though we recreate "alive" node when reconnecting it might be not enough and race condition is possible.
            /// And everything we can do here - just retry.
            unready_host_state.alive = zk_nodes_set.contains(alive_node_name);
            if (!unready_host_state.alive)
            {
                LOG_TRACE(log, "Seems like host ({}) is dead. Will retry the check to confirm", host);
                auto zookeeper_retries_info = global_zookeeper_retries_info;
                ZooKeeperRetriesControl retries_ctl("waitImpl::checkAliveNode ", zookeeper_retries_info);
                retries_ctl.retryLoop(
                    [&]()
                    {
                        zookeeper = get_zookeeper();
                        if (zookeeper->exists(alive_node_path))
                        {
                            unready_host_state.alive = true;
                            return;
                        }

                        if (!retries_ctl.isLastRetry())
                            retries_ctl.requestUnconditionalRetry();
                    });
            }
            LOG_TRACE(log, "Host ({}) appeared to be {}", host, unready_host_state.alive ? "alive" : "dead");

            state.unready_hosts.emplace(host, unready_host_state);
            if (!unready_host_state.alive && unready_host_state.started && !state.host_terminated)
                state.host_terminated = host;
        }
    }

    if (state.host_terminated || !state.unready_hosts.empty())
        return state;

    auto zookeeper_retries_info = global_zookeeper_retries_info;
    ZooKeeperRetriesControl retries_ctl("waitImpl::collectStagesToWait ", zookeeper_retries_info);
    retries_ctl.retryLoop(
        [&]()
        {
            zookeeper = get_zookeeper();
            Strings results;

            for (const auto & host : all_hosts)
                results.emplace_back(zookeeper->get(zookeeper_path + "/current|" + host + "|" + stage_to_wait));

            state.results = std::move(results);
        });

    return state;
}

Strings BackupCoordinationStageSync::waitImpl(
    const Strings & all_hosts, const String & stage_to_wait, std::optional<std::chrono::milliseconds> timeout) const
{
    if (all_hosts.empty())
        return {};

    /// Wait until all hosts are ready or an error happens or time is out.

    /// Set by ZooKepper when list of zk nodes have changed.
    auto watch = std::make_shared<Poco::Event>();

    bool use_timeout = timeout.has_value();
    std::chrono::steady_clock::time_point end_of_timeout;
    if (use_timeout)
        end_of_timeout = std::chrono::steady_clock::now() + std::chrono::duration_cast<std::chrono::steady_clock::duration>(*timeout);

    State state;
    String previous_unready_host; /// Used for logging: we don't want to log the same unready host again.
    auto zookeeper = get_zookeeper();

    for (;;)
    {
        LOG_INFO(log, "Waiting for the stage {}", stage_to_wait);

        Strings zk_nodes;
        auto zookeeper_retries_info = global_zookeeper_retries_info;
        ZooKeeperRetriesControl retries_ctl("waitImpl::getChildren", zookeeper_retries_info);
        retries_ctl.retryLoop(
            [&]()
            {
                zookeeper = get_zookeeper();
                /// Get zk nodes and subscribe on their changes.
                zk_nodes = zookeeper->getChildren(zookeeper_path, nullptr, watch);
            });

        /// Read and analyze the current state of zk nodes.
        state = readCurrentState(zookeeper, zk_nodes, all_hosts, stage_to_wait);
        if (state.error || state.host_terminated || state.unready_hosts.empty())
            break; /// Error happened or everything is ready.

        /// Log that we will wait for another host.
        const auto & unready_host = state.unready_hosts.begin()->first;
        if (unready_host != previous_unready_host)
        {
            LOG_TRACE(log, "Waiting for host {}", unready_host);
            previous_unready_host = unready_host;
        }

        /// Wait until `watch_callback` is called by ZooKeeper meaning that zk nodes have changed.
        {
            if (use_timeout)
            {
                auto current_time = std::chrono::steady_clock::now();
                if ((current_time > end_of_timeout)
                    || !watch->tryWait(std::chrono::duration_cast<std::chrono::milliseconds>(end_of_timeout - current_time).count()))
                    break;
            }
            else
            {
                watch->wait();
            }
        }
    }

    /// Rethrow an error raised originally on another host.
    if (state.error)
        state.error->second.rethrow();

    /// Another host terminated without errors.
    if (state.host_terminated)
        throw Exception(ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE, "Host {} suddenly stopped working", *state.host_terminated);

    /// Something's unready, timeout is probably not enough.
    if (!state.unready_hosts.empty())
    {
        const auto & [unready_host, unready_host_state] = *state.unready_hosts.begin();
        throw Exception(
            ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE,
            "Waited for host {} too long (> {}){}",
            unready_host,
            to_string(*timeout),
            unready_host_state.started ? "" : ": Operation didn't start");
    }

    return state.results;
}

}
