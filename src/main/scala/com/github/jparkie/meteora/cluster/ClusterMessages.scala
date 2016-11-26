package com.github.jparkie.meteora.cluster

import akka.actor.Address
import com.github.jparkie.meteora.core.{ MeteoraToken, MeteoraTokenRange }

object ClusterMessages {
  sealed trait ClusterMessage extends Serializable {
    val messageTimestamp: Long = System.currentTimeMillis()
  }

  sealed trait ClusterCommand extends ClusterMessage

  sealed trait ClusterEvent extends ClusterMessage

  sealed trait ClusterQuery extends ClusterMessage

  sealed trait ClusterAnswer extends ClusterMessage

  case class GetTokenRangeForTokenClusterQuery(
    token: MeteoraToken
  ) extends ClusterQuery

  case class GetTokenRangesForNodeClusterQuery(
    node: Address
  ) extends ClusterQuery

  case class GetNodeForTokenClusterQuery(
    token: MeteoraToken
  ) extends ClusterQuery

  case class GetNodeForTokenRangeClusterQuery(
    tokenRange: MeteoraTokenRange
  ) extends ClusterQuery

  case class GetPreferenceListForTokenClusterQuery(
    token:         MeteoraToken,
    numOfReplicas: Int
  ) extends ClusterQuery

  case class GetPreferenceListForTokenRangeClusterQuery(
    tokenRange:    MeteoraTokenRange,
    numOfReplicas: Int
  ) extends ClusterQuery

  case class GetReachablePreferenceListForTokenClusterQuery(
    token:         MeteoraToken,
    numOfReplicas: Int
  ) extends ClusterQuery

  case class GetReachablePreferenceListForTokenRangeClusterQuery(
    tokenRange:    MeteoraTokenRange,
    numOfReplicas: Int
  ) extends ClusterQuery

  case class GetTokenRangeForTokenClusterAnswer(
    tokenRange: MeteoraTokenRange
  ) extends ClusterAnswer

  case class GetTokenRangesForNodeClusterAnswer(
    tokenRanges: List[MeteoraTokenRange]
  ) extends ClusterAnswer

  case class GetNodeForTokenClusterAnswer(
    node: Address
  ) extends ClusterAnswer

  case class GetNodeForTokenRangeClusterAnswer(
    node: Address
  ) extends ClusterAnswer

  case class GetPreferenceListForTokenClusterAnswer(
    nodes: List[Address]
  ) extends ClusterAnswer

  case class GetPreferenceListForTokenRangeClusterAnswer(
    nodes: List[Address]
  ) extends ClusterAnswer

  case class GetReachablePreferenceListForTokenClusterAnswer(
    nodes: List[Address]
  ) extends ClusterAnswer

  case class GetReachablePreferenceListForTokenRangeClusterAnswer(
    nodes: List[Address]
  ) extends ClusterAnswer
}
