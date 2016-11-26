package com.github.jparkie.meteora.storage.rocksdb

import java.io.File
import java.util.concurrent.locks.Lock

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.github.jparkie.meteora.core.{ MeteoraEntry, MeteoraToken, MeteoraTokenRange, MeteoraTuple }
import com.github.jparkie.meteora.storage.Store
import com.github.jparkie.meteora.util.{ MeteoraException, MeteoraFuture, MeteoraTry }
import com.google.common.util.concurrent.Striped
import org.rocksdb._

import scala.collection.JavaConversions._
import scala.concurrent.Future
import scala.util.Try

trait RocksDBStore extends Store with RocksDBStoreBase {
  private var rockLocks: Striped[Lock] = _
  private var rocksDB: RocksDB = _

  private var dbOptions: DBOptions = _
  private var comparatorOptions: ComparatorOptions = _
  private var comparator: Comparator = _
  private var cfOptions: ColumnFamilyOptions = _
  private var cfDescriptors: JavaList[ColumnFamilyDescriptor] = _
  private var cfHandles: JavaList[ColumnFamilyHandle] = _

  override def open(): Try[Unit] = MeteoraTry {
    if (rocksDB == null) {
      RocksDB.loadLibrary()
      val path = meteoraConfig.rocksDirectory
      new File(path).mkdirs()
      dbOptions = createDBOptions()
      comparatorOptions = new ComparatorOptions()
      comparator = new RocksDBComparator(comparatorOptions)
      cfOptions = createCFOptions(comparator)
      cfDescriptors = createCFDescriptors(cfOptions)
      cfHandles = createCFHandles()
      rockLocks = createRockLocks()
      rocksDB = RocksDB.open(dbOptions, path, cfDescriptors, cfHandles)
      // RocksDB requires the default column family; It is not used, so dispose immediately.
      cfDescriptors.remove(DEFAULT_COLUMN_FAMILY_INDEX)
      cfHandles.remove(DEFAULT_COLUMN_FAMILY_INDEX).close()
    } else {
      throw new MeteoraException("Failed to open RocksDB because it is already opened")
    }
  }

  override def close(): Try[Unit] = MeteoraTry {
    if (rocksDB != null) {
      rocksDB.pauseBackgroundWork()
      cfHandles.foreach(currentCFHandle => currentCFHandle.close())
      cfOptions.close()
      comparator.close()
      comparatorOptions.close()
      dbOptions.close()
      rocksDB.close()
      rocksDB = null
      rockLocks = null
      cfHandles = null
      cfOptions = null
      comparator = null
      comparatorOptions = null
      dbOptions = null
    } else {
      throw new MeteoraException("Failed to close RocksDB because it is already closed")
    }
  }

  override def set(token: MeteoraToken, tuple: MeteoraTuple): Future[Unit] = MeteoraFuture {
    val cfHandle = internalCFHandle(token.toMeteoraTokenRange)
    val keyBytes = fromMeteoraToken(token)
    val lock = rockLocks.get(getStripedKey(token))
    val lockLength = meteoraConfig.rocksLockTimeout.length
    val lockUnit = meteoraConfig.rocksLockTimeout.unit
    if (lock.tryLock(lockLength, lockUnit)) {
      try {
        val oldValueBytes = rocksDB.get(cfHandle, keyBytes)
        if (oldValueBytes != null) {
          val oldTuple = toMeteoraTuple(oldValueBytes)
          if (tuple.timestamp > oldTuple.timestamp) {
            val valueBytes = fromMeteoraTuple(tuple)
            rocksDB.put(cfHandle, keyBytes, valueBytes)
          } else {
            throw new MeteoraException(
              "Failed to set as the proposed tuple was rejected " +
                "by the last-write-win conflict resolution policy"
            )
          }
        } else {
          val valueBytes = fromMeteoraTuple(tuple)
          rocksDB.put(cfHandle, keyBytes, valueBytes)
        }
      } finally {
        lock.unlock()
      }
    } else {
      throw new MeteoraException(s"Failed to acquire a lock after $lockLength ms")
    }
  }

  override def get(token: MeteoraToken): Future[MeteoraEntry] = MeteoraFuture {
    val cfHandle = internalCFHandle(token.toMeteoraTokenRange)
    val keyBytes = fromMeteoraToken(token)
    val valueBytes = rocksDB.get(cfHandle, keyBytes)
    if (valueBytes != null) {
      val tuple = toMeteoraTuple(valueBytes)
      MeteoraEntry(token, tuple)
    } else {
      throw new MeteoraException("Failed to find a tuple for the provided token")
    }
  }

  override def tokens(tokenRange: MeteoraTokenRange): Source[MeteoraToken, NotUsed] = {
    val cfHandle = internalCFHandle(tokenRange)
    val rocksIterator = rocksDB.newIterator(cfHandle)
    val rocksSource = Source.fromGraph(new RangedRocksDBTokenSource(rocksIterator, tokenRange))
    rocksSource
  }

  override def entries(tokenRange: MeteoraTokenRange): Source[MeteoraEntry, NotUsed] = {
    val cfHandle = internalCFHandle(tokenRange)
    val rocksIterator = rocksDB.newIterator(cfHandle)
    val rocksSource = Source.fromGraph(new RangedRocksDBEntrySource(rocksIterator, tokenRange))
    rocksSource
  }

  override def tokens(): Source[MeteoraToken, NotUsed] = {
    val rocksIterator = rocksDB.newIterator()
    val rocksSource = Source.fromGraph(new RocksDBTokenSource(rocksIterator))
    rocksSource
  }

  override def entries(): Source[MeteoraEntry, NotUsed] = {
    val rocksIterator = rocksDB.newIterator()
    val rocksSource = Source.fromGraph(new RocksDBEntrySource(rocksIterator))
    rocksSource
  }

  override def drop(tokenRange: MeteoraTokenRange): Future[Unit] = MeteoraFuture {
    val cfHandle = internalCFHandle(tokenRange)
    val cfHandleIndex = cfHandles.indexOf(cfHandle)
    val cfDescriptor = cfDescriptors.get(cfHandleIndex)
    rocksDB.dropColumnFamily(cfHandle)
    val newCFHandle = rocksDB.createColumnFamily(cfDescriptor)
    cfHandles.set(cfHandleIndex, newCFHandle)
  }

  private def internalCFHandle(tokenRange: MeteoraTokenRange): ColumnFamilyHandle = {
    import MeteoraTokenRange._
    val trIndex = tokenRangeIndex(tokenRange, tokenRangeRing.tokenRanges)
    cfHandles(trIndex)
  }
}