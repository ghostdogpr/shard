package shard

import zio._

/**
 * Sharding configuration
 * @param role role of the current pod
 * @param numberOfShards number of shards (see documentation on how to choose this), should be same on all nodes
 * @param selfHost hostname or IP address of the current pod
 * @param shardingPort port used for pods to communicate together
 * @param shardManagerHost hostname or IP address of the Shard Manager
 * @param shardManagerPort port of the Shard Manager gRPC API
 * @param shardManagerTimeout timeout for calls to the Shard Manager
 * @param serverVersion version of the current pod
 * @param entityMaxIdleTime time of inactivity (without processing any message) after which an entity will be stopped
 * @param entityTerminationTimeout time we give to an entity to finish processing its messages when it is stopped
 * @param sendTimeout default timeout when sending a message
 * @param refreshAssignmentsRetryInterval retry interval in case of failure getting shard assignments from storage
 * @param unhealthyPodReportInterval interval to report unhealthy pods to the Shard Manager (this exists to prevent calling the Shard Manager for each failed message)
 * @param simulateRemotePods disable optimizations when sending a message to an entity hosted on the local shards (this will force serialization of all messages)
 * @param unregisterRetrySchedule retry schedule for unregistering the pod from the Shard Manager
 * @param entityInterceptor interceptor applied to the entities of every type, around the one of their `EntityOptions`
 */
case class Config(
  role: Role,
  numberOfShards: Int,
  selfHost: String,
  shardingPort: Int,
  shardManagerHost: String,
  shardManagerPort: Int,
  shardManagerTimeout: Duration,
  serverVersion: String,
  entityMaxIdleTime: Duration,
  entityTerminationTimeout: Duration,
  sendTimeout: Duration,
  refreshAssignmentsRetryInterval: Duration,
  unhealthyPodReportInterval: Duration,
  simulateRemotePods: Boolean,
  unregisterRetrySchedule: Schedule[Any, Any, Any],
  entityInterceptor: EntityInterceptor = EntityInterceptor.none
)

object Config {
  val default: Config = Config(
    role = Role.default,
    numberOfShards = 300,
    selfHost = "localhost",
    shardingPort = 54321,
    shardManagerHost = "localhost",
    shardManagerPort = 8080,
    shardManagerTimeout = 1 minute,
    serverVersion = "1.0.0",
    entityMaxIdleTime = 1 minute,
    entityTerminationTimeout = 3 seconds,
    sendTimeout = 10 seconds,
    refreshAssignmentsRetryInterval = 5 seconds,
    unhealthyPodReportInterval = 5 seconds,
    simulateRemotePods = false,
    unregisterRetrySchedule = Schedule.spaced(3.second) && Schedule.recurs(5)
  )
}
