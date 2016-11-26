package com.github.jparkie.meteora.util.config

import java.util.Properties
import java.util.concurrent.TimeUnit

import scala.collection.JavaConversions._
import scala.concurrent.duration._

trait RocksDBConfig extends ConfigBase {
  import RocksDBConfig._

  lazy val rocksDBOptions: Properties = {
    val properties = new Properties()
    if (config.hasPath(RocksDBOptions)) {
      config.getConfig(RocksDBOptions).entrySet().foreach { currentEntry =>
        properties.put(currentEntry.getKey, currentEntry.getValue.toString)
      }
    }
    properties
  }

  lazy val rocksCFOptions: Properties = {
    val properties = new Properties()
    if (config.hasPath(RocksCFOptions)) {
      config.getConfig(RocksCFOptions).entrySet().foreach { currentEntry =>
        properties.put(currentEntry.getKey, currentEntry.getValue.toString)
      }
    }
    properties
  }

  lazy val rocksDirectory: String = {
    config.getString(RocksDirectory)
  }

  lazy val rocksLockStripes: Int = {
    config.getInt(RocksLockStripes)
  }

  lazy val rocksLockTimeout: FiniteDuration = {
    FiniteDuration(config.getDuration(RocksLockTimeout, TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
  }
}

object RocksDBConfig {
  val RocksDBOptions = "meteora.rocksdb.db.options"
  val RocksCFOptions = "meteora.rocksdb.cf.options"
  val RocksDirectory = "meteora.rocksdb.dir"
  val RocksLockStripes = "meteora.rocksdb.lock.stripes"
  val RocksLockTimeout = "meteora.rocksdb.lock.timeout"
}