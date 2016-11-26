package com.github.jparkie.meteora.core

import com.google.common.base.Charsets
import com.google.common.hash.{ HashCode, HashFunction, Hashing }

case class MeteoraToken(value: BigInt) extends Serializable {
  import MeteoraToken._

  require(
    MinimumInclusiveValue <= value && value <= MaximumExclusiveValue,
    "MeteoraToken must be MinimumInclusiveValue <= value <= MaximumExclusiveValue"
  )

  def compare(that: MeteoraToken): Int = this.value.compare(that.value)

  def <=(that: MeteoraToken): Boolean = compare(that) <= 0

  def >=(that: MeteoraToken): Boolean = compare(that) >= 0

  def <(that: MeteoraToken): Boolean = compare(that) < 0

  def >(that: MeteoraToken): Boolean = compare(that) > 0

  def +(that: MeteoraToken): MeteoraToken = MeteoraToken(this.value + that.value)

  def -(that: MeteoraToken): MeteoraToken = MeteoraToken(this.value - that.value)

  def *(that: MeteoraToken): MeteoraToken = MeteoraToken(this.value * that.value)

  def /(that: MeteoraToken): MeteoraToken = MeteoraToken(this.value / that.value)

  def %(that: MeteoraToken): MeteoraToken = MeteoraToken(this.value % that.value)

  def /%(that: MeteoraToken): (MeteoraToken, MeteoraToken) = {
    val (d, r) = this.value /% that.value
    (MeteoraToken(d), MeteoraToken(r))
  }

  def min(that: MeteoraToken): MeteoraToken = MeteoraToken(this.value.min(that.value))

  def max(that: MeteoraToken): MeteoraToken = MeteoraToken(this.value.max(that.value))

  def until(end: MeteoraToken): MeteoraTokenRange = MeteoraTokenRange(this, end)

  def toMeteoraTokenRange: MeteoraTokenRange = {
    if (this.value == MaximumExclusiveValue) {
      return MeteoraTokenRange(0, 1)
    }
    MeteoraTokenRange(this, this + 1)
  }

  lazy val toByteArray: Array[Byte] = this.value.toByteArray
}

object MeteoraToken {
  def apply(key: String): MeteoraToken = {
    fromHashCode(hashFunction.hashString(key, Charsets.UTF_8))
  }

  def fromByteArray(byteArray: Array[Byte]): MeteoraToken = {
    MeteoraToken(BigInt(byteArray))
  }

  def hashFunction: HashFunction = {
    Hashing.murmur3_128()
  }

  def toHashCode(token: MeteoraToken): HashCode = {
    val toLittleEndianBytes = token.toByteArray.reverse
    HashCode.fromBytes(toLittleEndianBytes)
  }

  def fromHashCode(hashCode: HashCode): MeteoraToken = {
    val toBigEndianBytes = hashCode.asBytes.reverse
    MeteoraToken.fromByteArray(toBigEndianBytes)
  }

  def toHashString(hashCode: HashCode): String = {
    hashCode.toString
  }

  def fromHashString(hashString: String): HashCode = {
    HashCode.fromString(hashString)
  }

  val MeteoraTokenBits = hashFunction.bits()

  val MinimumInclusiveValue = BigInt(2).pow(MeteoraTokenBits - 1) * -1

  val MaximumExclusiveValue = BigInt(2).pow(MeteoraTokenBits - 1)

  val MinimumInclusiveToken = MeteoraToken(MinimumInclusiveValue)

  val MaximumExclusiveToken = MeteoraToken(MaximumExclusiveValue)

  val MeteoraKeyspace = MeteoraTokenRange(MinimumInclusiveToken, MaximumExclusiveToken)

  implicit def int2MeteoraToken(int: Int): MeteoraToken = MeteoraToken(BigInt(int))

  implicit def long2MeteoraToken(long: Long): MeteoraToken = MeteoraToken(BigInt(long))

  implicit def bigInt2MeteoraToken(bigInt: BigInt): MeteoraToken = MeteoraToken(bigInt)

  implicit object MeteoraTokenOrdering extends Ordering[MeteoraToken] {
    override def compare(left: MeteoraToken, right: MeteoraToken): Int = {
      left.compare(right)
    }
  }
}

