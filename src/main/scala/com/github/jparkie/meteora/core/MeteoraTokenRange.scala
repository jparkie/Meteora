package com.github.jparkie.meteora.core

import scala.collection.mutable
import scala.math.Ordering

case class MeteoraTokenRange(inclusiveMin: MeteoraToken, exclusiveMax: MeteoraToken) extends Serializable {
  import MeteoraToken._

  require(inclusiveMin < exclusiveMax, "MeteoraTokenRange must have inclusiveMin < exclusiveMax")

  lazy val size: BigInt = exclusiveMax.value - inclusiveMin.value

  def contains(token: MeteoraToken): Boolean = {
    inclusiveMin <= token && token < exclusiveMax
  }

  def contains(tokenRange: MeteoraTokenRange): Boolean = {
    inclusiveMin <= tokenRange.inclusiveMin && tokenRange.exclusiveMax <= exclusiveMax
  }

  def split(): (MeteoraTokenRange, MeteoraTokenRange) = {
    val midpoint = inclusiveMin + (size / 2)
    (inclusiveMin until midpoint, midpoint until exclusiveMax)
  }

  def merge(that: MeteoraTokenRange): MeteoraTokenRange = {
    MeteoraTokenRange(this.inclusiveMin, that.exclusiveMax)
  }
}

object MeteoraTokenRange {
  def splitTokenRange(tokenRange: MeteoraTokenRange, numOfSplits: Int): Array[MeteoraTokenRange] = {
    val numOfTokens = math.pow(2, numOfSplits).toInt
    val queue = mutable.Queue[MeteoraTokenRange](tokenRange)
    while (queue.size < numOfTokens) {
      val currentToken = queue.dequeue()
      val (leftToken, rightToken) = currentToken.split()
      queue.enqueue(leftToken, rightToken)
    }
    queue.toArray
  }

  def calculateTokenRangeSplits(numOfTokenRanges: Int): Int = {
    math.ceil(math.log(numOfTokenRanges) / math.log(2)).toInt
  }

  def tokenRangeIndex(
    tokenRange:  MeteoraTokenRange,
    tokenRanges: IndexedSeq[MeteoraTokenRange]
  )(implicit ord: Ordering[MeteoraTokenRange]): Int = {
    import scala.collection.Searching._
    val searchResult = search(tokenRanges).search(tokenRange)
    val ipIndex = insertionPointIndex(searchResult.insertionPoint)
    val rIndex = ringIndex(ipIndex, tokenRanges.length)
    rIndex
  }

  def insertionPointIndex(index: Int): Int = {
    if (index >= 0) {
      index
    } else {
      math.abs(index + 1)
    }
  }

  def ringIndex(index: Int, length: Int): Int = {
    index % length
  }

  implicit object MeteoraTokenRangeOrdering extends Ordering[MeteoraTokenRange] {
    override def compare(left: MeteoraTokenRange, right: MeteoraTokenRange): Int = {
      val minCompare = left.inclusiveMin.compare(right.inclusiveMin)
      if (minCompare != 0) {
        return minCompare
      }
      val maxCompare = left.exclusiveMax.compare(right.exclusiveMax)
      if (maxCompare != 0) {
        return maxCompare
      }
      0
    }
  }
}