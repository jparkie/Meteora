package com.github.jparkie.meteora.repair.merkle

import com.github.jparkie.meteora.core.{ MeteoraEntry, MeteoraTokenRange, MeteoraTuple }
import com.google.common.base.Charsets
import com.google.common.hash.Hashing

import scala.collection.mutable

class MerkleTreeBuilder(tokenRange: MeteoraTokenRange, numOfPartitions: Int) {
  import MeteoraTokenRange._

  private val numOfSplits: Int = calculateTokenRangeSplits(numOfPartitions)
  private val tokenRangesArray: Array[MeteoraTokenRange] = splitTokenRange(tokenRange, numOfSplits)
  private val merkleLeafsArray: Array[MerkleLeaf] = tokenRangesArray
    .map(currentTokenRange => MerkleLeaf(currentTokenRange, Array.emptyByteArray))

  private var currentPosition = 0

  def addEntry(entry: MeteoraEntry): Unit = {
    while (currentPosition < tokenRangesArray.length && !tokenRangesArray(currentPosition).contains(entry.token)) {
      currentPosition += 1
    }
    val currentMerkleLeaf = merkleLeafsArray(currentPosition)
    val newHash = hash(entry.tuple)
    val combinedHash = binaryHash(currentMerkleLeaf.hash, newHash)
    merkleLeafsArray(currentPosition) = currentMerkleLeaf.copy(hash = combinedHash)
  }

  def build(): MerkleTree = {
    val queue = new mutable.Queue[MerkleTree]()
    merkleLeafsArray.foreach { merkleLeaf =>
      queue.enqueue(merkleLeaf)
    }
    while (queue.size > 1) {
      val left = queue.dequeue()
      val right = queue.dequeue()
      val combinedHash = binaryHash(left.hash, right.hash)
      val combinedTokenRange = left.tokenRange merge right.tokenRange
      queue.enqueue(MerkleBranch(combinedTokenRange, combinedHash, left, right))
    }
    queue.dequeue()
  }

  def clear(): Unit = {
    val merkleLeafsArrayLength = merkleLeafsArray.length
    for (index <- 0 until merkleLeafsArrayLength) {
      merkleLeafsArray(index) = merkleLeafsArray(index).copy(hash = Array.emptyByteArray)
    }
    currentPosition = 0
  }

  private def hash(tuple: MeteoraTuple): Array[Byte] = {
    Hashing.murmur3_32()
      .newHasher()
      .putString(tuple.key, Charsets.UTF_8)
      .putString(tuple.value, Charsets.UTF_8)
      .putLong(tuple.timestamp)
      .hash()
      .asBytes()
  }

  private def binaryHash(leftHash: Array[Byte], rightHash: Array[Byte]): Array[Byte] = {
    Hashing.murmur3_32()
      .newHasher()
      .putBytes(leftHash)
      .putBytes(rightHash)
      .hash()
      .asBytes()
  }
}
