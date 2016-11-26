package com.github.jparkie.meteora.repair.merkle

import com.github.jparkie.meteora.core.MeteoraTokenRange

sealed trait MerkleTree extends Serializable {
  val tokenRange: MeteoraTokenRange
  val hash: Array[Byte]
}

case class MerkleBranch(
  tokenRange: MeteoraTokenRange,
  hash:       Array[Byte],
  left:       MerkleTree,
  right:      MerkleTree
) extends MerkleTree

case class MerkleLeaf(
  tokenRange: MeteoraTokenRange,
  hash:       Array[Byte]
) extends MerkleTree