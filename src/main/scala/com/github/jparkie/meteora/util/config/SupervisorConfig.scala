package com.github.jparkie.meteora.util.config

import java.util.concurrent.TimeUnit

import scala.concurrent.duration._

trait SupervisorConfig extends ConfigBase {
  import SupervisorConfig._

  lazy val supervisorMinBackoff: FiniteDuration = {
    FiniteDuration(config.getDuration(SupervisorMinBackoff, TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
  }

  lazy val supervisorMaxBackoff: FiniteDuration = {
    FiniteDuration(config.getDuration(SupervisorMaxBackoff, TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
  }

  lazy val supervisorRandomFactor: Double = {
    config.getDouble(SupervisorRandomFactor)
  }
}

object SupervisorConfig {
  val SupervisorMinBackoff = "meteora.supervisor.minbackoff"
  val SupervisorMaxBackoff = "meteora.supervisor.maxbackoff"
  val SupervisorRandomFactor = "meteora.supervisor.randomfactor"
}