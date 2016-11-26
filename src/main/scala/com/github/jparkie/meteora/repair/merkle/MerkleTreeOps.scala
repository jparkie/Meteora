package com.github.jparkie.meteora.repair.merkle

import com.github.jparkie.meteora.core.MeteoraTokenRange
import com.google.common.base.{ Optional, Predicate }
import com.google.common.collect.BinaryTreeTraverser

import scala.collection.mutable

object MerkleTreeOps extends BinaryTreeTraverser[MerkleTree] {
  override def leftChild(root: MerkleTree): Optional[MerkleTree] = root match {
    case MerkleBranch(tokenRange, hash, left, right) =>
      Optional.of(left)
    case MerkleLeaf(tokenRange, hash) =>
      Optional.absent()
  }

  override def rightChild(root: MerkleTree): Optional[MerkleTree] = root match {
    case MerkleBranch(tokenRange, hash, left, right) =>
      Optional.of(right)
    case MerkleLeaf(tokenRange, hash) =>
      Optional.absent()
  }

  def aligned(left: MerkleTree, right: MerkleTree): Boolean = {
    left.tokenRange.inclusiveMin == right.tokenRange.inclusiveMin &&
      left.tokenRange.exclusiveMax == right.tokenRange.exclusiveMax
  }

  def equivalent(left: MerkleTree, right: MerkleTree): Boolean = {
    aligned(left, right) && (left.hash sameElements right.hash)
  }

  /**
   * Adapted from https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/utils/MerkleTree.java
   */
  def diff(left: MerkleTree, right: MerkleTree): List[MeteoraTokenRange] = {
    import InternalDiff._
    if (!aligned(left, right)) {
      throw new IllegalArgumentException("MerkleTrees must be aligned to diff")
    }
    val diffs = mutable.ListBuffer.empty[MeteoraTokenRange]
    val activeRange = left.tokenRange
    val lNode = internalFind(left, activeRange)
    val rNode = internalFind(left, activeRange)
    if (!internalEquals(lNode, rNode)) {
      if (FullyInconsistent == internalDiff(left, right, diffs, activeRange)) {
        diffs.append(activeRange)
      }
    } else if (lNode.isEmpty || rNode.isEmpty) {
      diffs.append(activeRange)
    }
    diffs.toList
  }

  private object InternalDiff {
    sealed trait Consistency
    case object Consistent extends Consistency
    case object FullyInconsistent extends Consistency
    case object PartiallyInconsistent extends Consistency

    def internalDiff(
      left:   MerkleTree,
      right:  MerkleTree,
      diffs:  mutable.ListBuffer[MeteoraTokenRange],
      active: MeteoraTokenRange
    ): Consistency = {
      val midpointToken = active.inclusiveMin + ((active.exclusiveMax - active.inclusiveMin) / 2)
      val lRange = MeteoraTokenRange(active.inclusiveMin, midpointToken)
      val rRange = MeteoraTokenRange(midpointToken, active.exclusiveMax)
      // Recursive Left Check:
      val llNode = internalFind(left, lRange)
      val lrNode = internalFind(right, lRange)
      val lResolution = llNode.isDefined && lrNode.isDefined
      val lDifference = {
        if (!internalEquals(llNode, lrNode)) {
          internalDiff(left, right, diffs, lRange)
        } else if (!lResolution) {
          FullyInconsistent
        } else {
          Consistent
        }
      }
      // Recursive Right Check:
      val rlNode = internalFind(left, rRange)
      val rrNode = internalFind(right, rRange)
      val rResolution = rlNode.isDefined && rrNode.isDefined
      val rDifference = {
        if (!internalEquals(rlNode, rrNode)) {
          internalDiff(left, right, diffs, rRange)
        } else if (!rResolution) {
          FullyInconsistent
        } else {
          Consistent
        }
      }
      // Consistency:
      if (lDifference == FullyInconsistent && rDifference == FullyInconsistent) {
        FullyInconsistent
      } else if (lDifference == FullyInconsistent) {
        diffs.append(lRange)
        PartiallyInconsistent
      } else if (rDifference == FullyInconsistent) {
        diffs.append(rRange)
        PartiallyInconsistent
      } else {
        PartiallyInconsistent
      }
    }

    def internalEquals(lNode: Option[MerkleTree], rNode: Option[MerkleTree]): Boolean = {
      lNode.isDefined && rNode.isDefined && (lNode.get.hash sameElements rNode.get.hash)
    }

    def internalFind(tree: MerkleTree, activeRange: MeteoraTokenRange): Option[MerkleTree] = {
      val internalFindPredicate = new InternalFindPredicate(activeRange)
      Option(breadthFirstTraversal(tree).firstMatch(internalFindPredicate).orNull())
    }

    private class InternalFindPredicate(activeRange: MeteoraTokenRange) extends Predicate[MerkleTree] {
      override def apply(input: MerkleTree): Boolean = {
        activeRange.contains(input.tokenRange)
      }
    }
  }
}
