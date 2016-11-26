package com.github.jparkie.meteora.util.config

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.FiniteDuration

trait StorageConfig extends ConfigBase {
  import StorageConfig._

  lazy val storageSubscribeTimeout: FiniteDuration = {
    FiniteDuration(config.getDuration(StorageSubscribeTimeout, TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
  }

  lazy val storeDispatcherId: String = StorageStoreDispatcher
}

object StorageConfig {
  val StorageSubscribeTimeout = "meteora.storage.subscribe-timeout"
  val StorageStoreDispatcher = "meteora.storage.store-dispatcher"
}
