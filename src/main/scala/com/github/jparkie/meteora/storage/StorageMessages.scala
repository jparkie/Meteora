package com.github.jparkie.meteora.storage

import akka.actor.ActorRef
import com.github.jparkie.meteora.core.{ MeteoraToken, MeteoraTokenRange, MeteoraTuple }
import com.github.jparkie.meteora.repair.merkle.MerkleTree

object StorageMessages {
  sealed trait StorageMessage extends Serializable {
    val messageTimestamp: Long = System.currentTimeMillis()
  }

  sealed trait StorageCommand extends StorageMessage

  sealed trait StorageEvent extends StorageMessage

  sealed trait StorageQuery extends StorageMessage

  sealed trait StorageAnswer extends StorageMessage

  case object PublishTokenRangeStreamOnInitStorageMessage extends StorageMessage

  case object PublishTokenRangeStreamAckStorageMessage extends StorageMessage

  case object PublishTokenRangeStreamOnCompleteStorageMessage extends StorageMessage

  case class PublishTokenRangeStreamTupleStorageMessage(
    token: MeteoraToken,
    tuple: MeteoraTuple
  ) extends StorageMessage

  case class SetTupleStorageCommand(
    token: MeteoraToken,
    tuple: MeteoraTuple
  ) extends StorageCommand

  case class RepairTupleStorageCommand(
    token: MeteoraToken,
    tuple: MeteoraTuple
  ) extends StorageCommand

  case class HashTokenRangeStorageCommand(
    tokenRange:      MeteoraTokenRange,
    numOfPartitions: Int
  ) extends StorageCommand

  case class SubscribeTokenRangeStreamStorageCommand(
    tokenRange: MeteoraTokenRange
  ) extends StorageCommand

  case class PublishTokenRangeStreamStorageCommand(
    tokenRange:         MeteoraTokenRange,
    subscriberActorRef: ActorRef
  ) extends StorageCommand

  case class SetTupleStorageEvent(
    token: MeteoraToken,
    tuple: MeteoraTuple
  ) extends StorageEvent

  case class RepairTupleStorageEvent(
    token: MeteoraToken,
    tuple: MeteoraTuple
  ) extends StorageEvent

  case class HashTokenRangeStorageEvent(
    tokenRange:      MeteoraTokenRange,
    numOfPartitions: Int,
    merkleTree:      MerkleTree
  ) extends StorageEvent

  case class SubscribeTokenRangeStreamStorageEvent(
    tokenRange:         MeteoraTokenRange,
    subscriberActorRef: ActorRef
  ) extends StorageEvent

  case class PublishTokenRangeStreamStorageEvent(
    tokenRange:         MeteoraTokenRange,
    subscriberActorRef: ActorRef
  ) extends StorageEvent

  case class GetTupleStorageQuery(
    token: MeteoraToken
  ) extends StorageQuery

  case class GetTupleStorageAnswer(
    token: MeteoraToken,
    tuple: MeteoraTuple
  ) extends StorageAnswer
}
