package com.github.jparkie.meteora.util.config

trait ClusterConfig extends ConfigBase {
  import ClusterConfig._

  lazy val numOfVirtualNodes: Int = {
    config.getInt(ClusterNumOfVirtualNodes)
  }
}

object ClusterConfig {
  val ClusterNumOfVirtualNodes = "meteora.cluster.num-of-virtual-nodes"
}