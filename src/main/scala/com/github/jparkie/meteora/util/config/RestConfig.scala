package com.github.jparkie.meteora.util.config

import java.util.concurrent.TimeUnit

import scala.concurrent.duration._

trait RestConfig extends ConfigBase {
  import RestConfig._

  lazy val restInterface: String = {
    config.getString(RestInterface)
  }

  lazy val restPort: Int = {
    config.getInt(RestPort)
  }

  lazy val restTimeout: FiniteDuration = {
    FiniteDuration(config.getDuration(RestTimeout, TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
  }
}

object RestConfig {
  val RestInterface = "meteora.rest.interface"
  val RestPort = "meteora.rest.port"
  val RestTimeout = "meteora.rest.timeout"
}
