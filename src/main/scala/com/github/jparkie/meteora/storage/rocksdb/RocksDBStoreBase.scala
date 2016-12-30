package com.github.jparkie.meteora.storage.rocksdb

import java.util.concurrent.locks.Lock

import akka.stream.stage.{ GraphStage, GraphStageLogic, OutHandler }
import akka.stream.{ Attributes, Outlet, SourceShape }
import com.github.jparkie.meteora.core.{ MeteoraEntry, MeteoraToken, MeteoraTokenRange }
import com.github.jparkie.meteora.storage.StoreBase
import com.github.jparkie.meteora.util.MeteoraException
import com.google.common.util.concurrent.Striped
import org.rocksdb._

import scala.collection.JavaConversions._

trait RocksDBStoreBase extends StoreBase { this: RocksDBStore =>
  import StoreBase._

  protected type JavaList[T] = java.util.List[T]

  protected val DEFAULT_COLUMN_FAMILY_INDEX = 0

  protected def createDBOptions(): DBOptions = {
    val dbOptions = {
      if (meteoraConfig.rocksDBOptions.nonEmpty) {
        DBOptions.getDBOptionsFromProps(meteoraConfig.rocksDBOptions)
      } else {
        new DBOptions()
      }
    }
    if (dbOptions == null) {
      throw new MeteoraException("Failed to create DBOptions for RocksDB")
    }
    dbOptions.setCreateIfMissing(true)
    dbOptions.setCreateMissingColumnFamilies(true)
    dbOptions.createStatistics()
    dbOptions
  }

  protected def createCFOptions(comparator: Comparator): ColumnFamilyOptions = {
    val cfOptions = {
      if (meteoraConfig.rocksCFOptions.nonEmpty) {
        ColumnFamilyOptions.getColumnFamilyOptionsFromProps(meteoraConfig.rocksCFOptions)
      } else {
        new ColumnFamilyOptions()
      }
    }
    if (cfOptions == null) {
      throw new MeteoraException("Failed to create CFOptions for RocksDB")
    }
    cfOptions.setComparator(comparator)
    cfOptions
  }

  protected def createCFDescriptors(cfOptions: ColumnFamilyOptions): JavaList[ColumnFamilyDescriptor] = {
    val cfDescriptors = new java.util.ArrayList[ColumnFamilyDescriptor]()
    cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOptions))
    tokenRangeRing.tokenRanges.foreach { tokenRange =>
      val cfNameBytes = fromMeteoraTokenRange(tokenRange)
      cfDescriptors.add(new ColumnFamilyDescriptor(cfNameBytes, cfOptions))
    }
    cfDescriptors
  }

  protected def createCFHandles(): JavaList[ColumnFamilyHandle] = {
    new java.util.ArrayList[ColumnFamilyHandle]()
  }

  protected def createRockLocks(): Striped[Lock] = {
    Striped.lock(meteoraConfig.rocksLockStripes)
  }

  protected def getStripedKey(token: MeteoraToken): Long = {
    import MeteoraToken._
    hashFunction.hashBytes(token.toByteArray).asLong()
  }

  class RocksDBComparator(comparatorOptions: ComparatorOptions) extends Comparator(comparatorOptions) {
    override def name(): String = "RocksDBComparator"

    override def compare(left: Slice, right: Slice): Int = {
      val leftBytes = left.data()
      val rightBytes = right.data()
      KeyByteArrayOrdering.compare(leftBytes, rightBytes)
    }
  }

  protected trait RocksIteratorGraphStage[T] extends GraphStage[SourceShape[T]] {
    val rocksDB: RocksDB
    val rocksSnapshot: Snapshot
    val rocksIterator: RocksIterator

    def preStartSeek(): Unit

    def hasNext: Boolean

    def next(): T

    val out: Outlet[T]

    override def shape: SourceShape[T] = SourceShape(out)

    @scala.throws[Exception](classOf[Exception])
    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
      new GraphStageLogic(shape) with OutHandler {
        @scala.throws[Exception](classOf[Exception])
        override def preStart(): Unit = {
          preStartSeek()
          super.preStart()
        }

        @scala.throws[Exception](classOf[Exception])
        override def postStop(): Unit = {
          rocksIterator.close()
          rocksDB.releaseSnapshot(rocksSnapshot)
          rocksSnapshot.close()
          super.postStop()
        }

        @scala.throws[Exception](classOf[Exception])
        override def onPull(): Unit = {
          if (hasNext) {
            push(out, next())
          } else {
            completeStage()
          }
        }

        setHandler(out, this)
      }
    }
  }

  protected class RocksDBTokenSource(
    val rocksDB:       RocksDB,
    val rocksSnapshot: Snapshot,
    val rocksIterator: RocksIterator
  ) extends RocksIteratorGraphStage[MeteoraToken] {
    override def preStartSeek(): Unit = {
      rocksIterator.seekToFirst()
    }

    override def hasNext: Boolean = {
      rocksIterator.isValid
    }

    override def next(): MeteoraToken = {
      val keyBytes = rocksIterator.key()
      toMeteoraToken(keyBytes)
    }

    override val out: Outlet[MeteoraToken] = Outlet("RocksDBTokenSource")
  }

  protected class RocksDBEntrySource(
    val rocksDB:       RocksDB,
    val rocksSnapshot: Snapshot,
    val rocksIterator: RocksIterator
  ) extends RocksIteratorGraphStage[MeteoraEntry] {
    override def preStartSeek(): Unit = {
      rocksIterator.seekToFirst()
    }

    override def hasNext: Boolean = {
      rocksIterator.isValid
    }

    override def next(): MeteoraEntry = {
      val keyBytes = rocksIterator.key()
      val valueBytes = rocksIterator.value()
      val token = toMeteoraToken(keyBytes)
      val tuple = toMeteoraTuple(valueBytes)
      MeteoraEntry(token, tuple)
    }

    override val out: Outlet[MeteoraEntry] = Outlet("RocksDBEntrySource")
  }

  protected class RangedRocksDBTokenSource(
    rocksDB:       RocksDB,
    rocksSnapshot: Snapshot,
    rocksIterator: RocksIterator,
    tokenRange:    MeteoraTokenRange
  ) extends RocksDBTokenSource(rocksDB, rocksSnapshot, rocksIterator) {
    val inclusiveMinBytes = fromMeteoraToken(tokenRange.inclusiveMin)
    val exclusiveMaxBytes = fromMeteoraToken(tokenRange.exclusiveMax)

    override def preStartSeek(): Unit = {
      rocksIterator.seek(inclusiveMinBytes)
    }

    override def hasNext: Boolean = {
      super.hasNext && KeyByteArrayOrdering.compare(rocksIterator.key(), exclusiveMaxBytes) < 0
    }

    override val out: Outlet[MeteoraToken] = Outlet("RangedRocksDBTokenSource")
  }

  protected class RangedRocksDBEntrySource(
    rocksDB:       RocksDB,
    rocksSnapshot: Snapshot,
    rocksIterator: RocksIterator,
    tokenRange:    MeteoraTokenRange
  ) extends RocksDBEntrySource(rocksDB, rocksSnapshot, rocksIterator) {
    val inclusiveMinBytes = fromMeteoraToken(tokenRange.inclusiveMin)
    val exclusiveMaxBytes = fromMeteoraToken(tokenRange.exclusiveMax)

    override def preStartSeek(): Unit = {
      rocksIterator.seek(inclusiveMinBytes)
    }

    override def hasNext: Boolean = {
      super.hasNext && KeyByteArrayOrdering.compare(rocksIterator.key(), exclusiveMaxBytes) < 0
    }

    override val out: Outlet[MeteoraEntry] = Outlet("RangedRocksDBEntrySource")
  }
}