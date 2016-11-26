package com.github.jparkie.meteora.util.config

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.FiniteDuration

trait RepairConfig extends ConfigBase {
  import RepairConfig._

  lazy val numOfPartitions: Int = {
    config.getInt(RepairNumOfPartitions)
  }

  lazy val totalTimeout: FiniteDuration = {
    FiniteDuration(
      config.getDuration(RepairTotalTimeout, TimeUnit.MILLISECONDS),
      TimeUnit.MILLISECONDS
    )
  }

  lazy val fetchTokenRangesTimeout: FiniteDuration = {
    FiniteDuration(
      config.getDuration(RepairFetchTokenRangesTimeout, TimeUnit.MILLISECONDS),
      TimeUnit.MILLISECONDS
    )
  }

  lazy val fetchReachablePreferenceListTimeout: FiniteDuration = {
    FiniteDuration(
      config.getDuration(RepairFetchReachablePreferenceListTimeout, TimeUnit.MILLISECONDS),
      TimeUnit.MILLISECONDS
    )
  }

  lazy val fetchStorageActorRefTimeout: FiniteDuration = {
    FiniteDuration(
      config.getDuration(RepairFetchStorageActorRefTimeout, TimeUnit.MILLISECONDS),
      TimeUnit.MILLISECONDS
    )
  }

  lazy val fetchMerkleTreeTimeout: FiniteDuration = {
    FiniteDuration(
      config.getDuration(RepairFetchMerkleTreeTimeout, TimeUnit.MILLISECONDS),
      TimeUnit.MILLISECONDS
    )
  }

  lazy val fetchSubscriberActorRefTimeout: FiniteDuration = {
    FiniteDuration(
      config.getDuration(RepairFetchSubscriberActorRefTimeout, TimeUnit.MILLISECONDS),
      TimeUnit.MILLISECONDS
    )
  }

  lazy val streamTokenRangeTimeout: FiniteDuration = {
    FiniteDuration(
      config.getDuration(RepairStreamTokenRangeTimeout, TimeUnit.MILLISECONDS),
      TimeUnit.MILLISECONDS
    )
  }
}

object RepairConfig {
  val RepairNumOfPartitions = "meteora.repair.num-of-partitions"
  val RepairTotalTimeout = "meteora.repair.total-timeout"
  val RepairFetchTokenRangesTimeout = "meteora.repair.fetch-token-ranges-timeout"
  val RepairFetchReachablePreferenceListTimeout = "meteora.repair.fetch-reachable-preference-list-timeout"
  val RepairFetchStorageActorRefTimeout = "meteora.repair.fetch-storage-actor-ref-timeout"
  val RepairFetchMerkleTreeTimeout = "meteora.repair.fetch-merkle-tree-timeout"
  val RepairFetchSubscriberActorRefTimeout = "meteora.repair.fetch-subscriber-actor-ref-timeout"
  val RepairStreamTokenRangeTimeout = "meteora.repair.stream-token-range-timeout"
}