package com.github.jparkie.meteora.core

class MeteoraTokenRangeRing private (val numOfVirtualNodes: Int) extends Serializable {
  import MeteoraToken._
  import MeteoraTokenRange._

  require(numOfVirtualNodes > 0, "The ring must have numOfVirtualNodes > 0")

  private val numOfSplits: Int = calculateTokenRangeSplits(numOfVirtualNodes)
  private val tokenRangeRingArray: Array[MeteoraTokenRange] = splitTokenRange(MeteoraKeyspace, numOfSplits)

  def tokenRanges: IndexedSeq[MeteoraTokenRange] = tokenRangeRingArray.toIndexedSeq

  def tokenRangeFor(token: MeteoraToken): MeteoraTokenRange = {
    val tokenRange = token.toMeteoraTokenRange
    val currentIndex = tokenRangeIndex(tokenRange, tokenRangeRingArray)
    tokenRangeRingArray(currentIndex)
  }
}

object MeteoraTokenRangeRing {
  def apply(numOfVirtualNodes: Int): MeteoraTokenRangeRing = {
    new MeteoraTokenRangeRing(numOfVirtualNodes)
  }
}
