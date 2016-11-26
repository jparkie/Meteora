package com.github.jparkie.meteora.core

import akka.actor.Address

import scala.collection.immutable

class MeteoraNodeRing private (
  val nodes:          immutable.SortedSet[Address],
  val tokenRangeRing: MeteoraTokenRangeRing
) extends Serializable {
  import MeteoraTokenRange._

  require(nodes.nonEmpty, "The ring must be non-empty")

  private val tokenRangeRingIndexedSeq: IndexedSeq[MeteoraTokenRange] = tokenRangeRing.tokenRanges
  private val nodeRingIndexedSeq: IndexedSeq[Address] = {
    val indexedNodes = nodes.toIndexedSeq
    val indexedNodeSize = nodes.size
    val nodeRing = for {
      (tokenRange, index) <- tokenRangeRing.tokenRanges.zipWithIndex
    } yield indexedNodes(index % indexedNodeSize)
    nodeRing
  }

  def :+(node: Address): MeteoraNodeRing = {
    new MeteoraNodeRing(nodes + node, tokenRangeRing)
  }

  def :-(node: Address): MeteoraNodeRing = {
    new MeteoraNodeRing(nodes - node, tokenRangeRing)
  }

  def tokenRangesFor(node: Address): List[MeteoraTokenRange] = {
    val nodeIndices = nodeRingIndexedSeq.zipWithIndex.collect {
      case (currentNode, currentIndex) if currentNode == node =>
        currentIndex
    }
    nodeIndices.map(tokenRangeRingIndexedSeq).toList
  }

  def nodeFor(token: MeteoraToken): Address = {
    val tokenRange = token.toMeteoraTokenRange
    val currentIndex = tokenRangeIndex(tokenRange, tokenRangeRingIndexedSeq)
    nodeRingIndexedSeq(currentIndex)
  }

  def nodeFor(tokenRange: MeteoraTokenRange): Address = {
    val currentIndex = tokenRangeIndex(tokenRange, tokenRangeRingIndexedSeq)
    nodeRingIndexedSeq(currentIndex)
  }

  def preferenceListFor(token: MeteoraToken, numOfReplicas: Int): List[Address] = {
    val replicaLimit = math.min(numOfReplicas, nodes.size)
    val tokenRange = token.toMeteoraTokenRange
    var currentIndex = tokenRangeIndex(tokenRange, tokenRangeRingIndexedSeq)
    var clockwiseReplicaSet = Set.empty[Address]
    while (clockwiseReplicaSet.size < replicaLimit) {
      clockwiseReplicaSet = clockwiseReplicaSet + nodeRingIndexedSeq(currentIndex)
      currentIndex = ringIndex(currentIndex + 1, tokenRangeRingIndexedSeq.length)
    }
    clockwiseReplicaSet.toList
  }

  def preferenceListFor(tokenRange: MeteoraTokenRange, numOfReplicas: Int): List[Address] = {
    val replicaLimit = math.min(numOfReplicas, nodes.size)
    var currentIndex = tokenRangeIndex(tokenRange, tokenRangeRingIndexedSeq)
    var clockwiseReplicaSet = Set.empty[Address]
    while (clockwiseReplicaSet.size < replicaLimit) {
      clockwiseReplicaSet = clockwiseReplicaSet + nodeRingIndexedSeq(currentIndex)
      currentIndex = ringIndex(currentIndex + 1, tokenRangeRingIndexedSeq.length)
    }
    clockwiseReplicaSet.toList
  }
}

object MeteoraNodeRing {
  def apply(nodes: Set[Address], tokenRangeRing: MeteoraTokenRangeRing): MeteoraNodeRing = {
    new MeteoraNodeRing(immutable.SortedSet.empty[Address] ++ nodes, tokenRangeRing)
  }

  implicit val addressOrdering: Ordering[Address] = Ordering.fromLessThan[Address] { (left, right) =>
    if (left eq right) {
      false
    } else if (left.host != right.host) {
      left.host.getOrElse("").compareTo(right.host.getOrElse("")) < 0
    } else if (left.port != right.port) {
      left.port.getOrElse(0) < right.port.getOrElse(0)
    } else {
      false
    }
  }
}
