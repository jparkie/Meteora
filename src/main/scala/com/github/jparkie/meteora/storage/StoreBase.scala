package com.github.jparkie.meteora.storage

import akka.actor.Actor
import akka.serialization.SerializationExtension
import akka.stream.ActorMaterializer
import com.github.jparkie.meteora.core.{ MeteoraToken, MeteoraTokenRange, MeteoraTuple }

trait StoreBase extends Actor { this: Store =>
  protected implicit val materializer = ActorMaterializer()
  protected implicit val serialization = SerializationExtension(context.system)
  protected val config = context.system.settings.config

  private lazy val meteoraTokenRangeSerializer = serialization
    .serializerFor(classOf[MeteoraTokenRange])
  private lazy val meteoraTupleSerializer = serialization
    .serializerFor(classOf[MeteoraTuple])

  protected def fromMeteoraToken(meteoraToken: MeteoraToken): Array[Byte] = {
    meteoraToken.toByteArray
  }

  protected def fromMeteoraTokenRange(meteoraTokenRange: MeteoraTokenRange): Array[Byte] = {
    meteoraTokenRangeSerializer.toBinary(meteoraTokenRange)
  }

  protected def fromMeteoraTuple(meteoraTuple: MeteoraTuple): Array[Byte] = {
    meteoraTupleSerializer.toBinary(meteoraTuple)
  }

  protected def toMeteoraToken(byteArray: Array[Byte]): MeteoraToken = {
    MeteoraToken.fromByteArray(byteArray)
  }

  protected def toMeteoraTokenRange(byteArray: Array[Byte]): MeteoraTokenRange = {
    meteoraTokenRangeSerializer.fromBinary(byteArray, Some(classOf[MeteoraTokenRange]))
      .asInstanceOf[MeteoraTokenRange]
  }

  protected def toMeteoraTuple(byteArray: Array[Byte]): MeteoraTuple = {
    meteoraTupleSerializer.fromBinary(byteArray, Some(classOf[MeteoraTuple]))
      .asInstanceOf[MeteoraTuple]
  }
}

object StoreBase {
  implicit object KeyByteArrayOrdering extends Ordering[Array[Byte]] {
    override def compare(left: Array[Byte], right: Array[Byte]): Int = {
      val leftToken = MeteoraToken.fromByteArray(left)
      val rightToken = MeteoraToken.fromByteArray(right)
      leftToken.compare(rightToken)
    }
  }
}