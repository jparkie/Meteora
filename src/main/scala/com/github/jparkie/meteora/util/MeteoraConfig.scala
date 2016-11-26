package com.github.jparkie.meteora.util

import akka.actor.ActorSystem.Settings
import com.github.jparkie.meteora.util.config._
import com.typesafe.config.Config

final class MeteoraConfig(val config: Config) extends Serializable
  with CoordinatorConfig
  with ClusterConfig
  with RepairConfig
  with RestConfig
  with RocksDBConfig
  with StorageConfig
  with SupervisorConfig {
  // Do Nothing.
}

object MeteoraConfig {
  def apply(config: Config): MeteoraConfig = {
    new MeteoraConfig(config)
  }

  def apply(settings: Settings): MeteoraConfig = {
    new MeteoraConfig(settings.config)
  }
}