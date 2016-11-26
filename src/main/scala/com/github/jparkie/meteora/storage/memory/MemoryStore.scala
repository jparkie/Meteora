package com.github.jparkie.meteora.storage.memory

import java.util.concurrent.locks.Lock

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.github.jparkie.meteora.core.{ MeteoraEntry, MeteoraToken, MeteoraTokenRange, MeteoraTuple }
import com.github.jparkie.meteora.storage.Store
import com.github.jparkie.meteora.util.{ MeteoraException, MeteoraFuture, MeteoraTry }
import com.google.common.collect.{ Table, TreeBasedTable }
import com.google.common.util.concurrent.Striped

import scala.collection.JavaConversions._
import scala.concurrent.Future
import scala.util.Try

trait MemoryStore extends Store with MemoryStoreBase {
  private var memoryLocks: Striped[Lock] = _
  private var memoryTable: Table[Int, MeteoraToken, MeteoraEntry] = _

  override def open(): Try[Unit] = MeteoraTry {
    if (memoryTable == null) {
      val rowOrdering = Ordering.Int
      val columnOrdering = MeteoraToken.MeteoraTokenOrdering
      memoryLocks = createMemoryLocks()
      memoryTable = TreeBasedTable.create(rowOrdering, columnOrdering)
    } else {
      throw new MeteoraException("Failed to open in-memory table because it is already opened")
    }
  }

  override def close(): Try[Unit] = MeteoraTry {
    if (memoryTable != null) {
      memoryTable.clear()
      memoryTable = null
      memoryLocks = null
    } else {
      throw new MeteoraException("Failed to close in-memory table because it is already closed")
    }
  }

  override def set(token: MeteoraToken, tuple: MeteoraTuple): Future[Unit] = MeteoraFuture {
    val rowIndex = internalRowIndex(token.toMeteoraTokenRange)
    val lock = memoryLocks.get(getStripedKey(token))
    if (lock.tryLock()) {
      try {
        val oldValue = memoryTable.get(rowIndex, token)
        val oldTuple = oldValue.tuple
        if (tuple.timestamp > oldTuple.timestamp) {
          val newEntry = MeteoraEntry(token, tuple)
          memoryTable.put(rowIndex, token, newEntry)
        } else {
          throw new MeteoraException(
            "Failed to set as the proposed tuple was rejected " +
              "by the last-write-win conflict resolution policy"
          )
        }
      } finally {
        lock.unlock()
      }
    } else {
      throw new MeteoraException(s"Failed to acquire a lock")
    }
  }

  override def get(token: MeteoraToken): Future[MeteoraEntry] = MeteoraFuture {
    val rowIndex = internalRowIndex(token.toMeteoraTokenRange)
    memoryTable.get(rowIndex, token)
  }

  override def tokens(tokenRange: MeteoraTokenRange): Source[MeteoraToken, NotUsed] = {
    val rowIndex = internalRowIndex(tokenRange)
    val keyIterator = memoryTable.row(rowIndex)
      .filterKeys(token => tokenRange.contains(token))
      .keysIterator
    Source.fromIterator(() => keyIterator)
  }

  override def entries(tokenRange: MeteoraTokenRange): Source[MeteoraEntry, NotUsed] = {
    val rowIndex = internalRowIndex(tokenRange)
    val valueIterator = memoryTable.row(rowIndex)
      .filterKeys(token => tokenRange.contains(token))
      .valuesIterator
    Source.fromIterator(() => valueIterator)
  }

  override def tokens(): Source[MeteoraToken, NotUsed] = {
    val keyIterator = memoryTable.columnKeySet().toIterator
    Source.fromIterator(() => keyIterator)
  }

  override def entries(): Source[MeteoraEntry, NotUsed] = {
    val valueIterator = memoryTable.values().toIterator
    Source.fromIterator(() => valueIterator)
  }

  override def drop(tokenRange: MeteoraTokenRange): Future[Unit] = MeteoraFuture {
    val rowIndex = internalRowIndex(tokenRange)
    memoryTable.remove(rowIndex, null)
  }

  private def internalRowIndex(tokenRange: MeteoraTokenRange): Int = {
    import MeteoraTokenRange._
    tokenRangeIndex(tokenRange, tokenRangeRing.tokenRanges)
  }
}
