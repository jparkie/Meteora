package com.github.jparkie.meteora.storage.memory

import java.util.concurrent.locks.Lock

import com.github.jparkie.meteora.core.MeteoraToken
import com.github.jparkie.meteora.storage.StoreBase
import com.google.common.util.concurrent.Striped

trait MemoryStoreBase extends StoreBase { this: MemoryStore =>
  protected def createMemoryLocks(): Striped[Lock] = {
    Striped.lock(meteoraConfig.rocksLockStripes)
  }

  protected def getStripedKey(token: MeteoraToken): Long = {
    import MeteoraToken._
    hashFunction.hashBytes(token.toByteArray).asLong()
  }
}
