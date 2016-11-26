package com.github.jparkie.meteora.util.config

import java.util.concurrent.TimeUnit

import scala.concurrent.duration._

trait CoordinatorConfig extends ConfigBase {
  import CoordinatorConfig._

  lazy val coordinatorReplicaConsistency: Int = {
    config.getInt(CoordinatorReplicaConsistency)
  }

  lazy val coordinatorSetTimeout: FiniteDuration = {
    FiniteDuration(config.getDuration(CoordinatorSetTimeout, TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
  }

  lazy val coordinatorGetTimeout: FiniteDuration = {
    FiniteDuration(config.getDuration(CoordinatorGetTimeout, TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
  }

  lazy val coordinatorReadConsistency: Int = {
    config.getInt(CoordinatorReadConsistency)
  }

  lazy val coordinatorReadConsistencyTimeout: FiniteDuration = {
    FiniteDuration(config.getDuration(CoordinatorReadTimeout, TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
  }

  lazy val coordinatorWriteConsistency: Int = {
    config.getInt(CoordinatorWriteConsistency)
  }

  lazy val coordinatorWriteConsistencyTimeout: FiniteDuration = {
    FiniteDuration(config.getDuration(CoordinatorWriteTimeout, TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
  }
}

object CoordinatorConfig {
  val CoordinatorSetTimeout = "meteora.coordinator.set-timeout"
  val CoordinatorGetTimeout = "meteora.coordinator.get-timeout"
  val CoordinatorReplicaConsistency = "meteora.coordinator.replicas-consistency"
  val CoordinatorReadConsistency = "meteora.coordinator.read.consistency"
  val CoordinatorReadTimeout = "meteora.coordinator.read.timeout"
  val CoordinatorWriteConsistency = "meteora.coordinator.write.consistency"
  val CoordinatorWriteTimeout = "meteora.coordinator.write.timeout"
}