package com.github.jparkie.meteora.coordinator

import com.github.jparkie.meteora.core.{ MeteoraToken, MeteoraTuple }

object CoordinatorMessages {
  sealed trait CoordinatorMessage extends Serializable {
    val messageTimestamp: Long = System.currentTimeMillis()
  }

  sealed trait CoordinatorCommand extends CoordinatorMessage

  sealed trait CoordinatorEvent extends CoordinatorMessage

  sealed trait CoordinatorQuery extends CoordinatorMessage

  sealed trait CoordinatorAnswer extends CoordinatorMessage

  case class SetTupleCoordinatorCommand(
    key:              String,
    value:            String,
    timestamp:        Long,
    writeConsistency: Int
  ) extends CoordinatorCommand

  case class SetTupleCoordinatorEvent(
    key:              String,
    value:            String,
    timestamp:        Long,
    writeConsistency: Int
  ) extends CoordinatorEvent

  case class GetTupleCoordinatorQuery(
    key:             String,
    readConsistency: Int
  ) extends CoordinatorQuery

  case class GetTupleCoordinatorAnswer(
    key:             String,
    value:           String,
    timestamp:       Long,
    readConsistency: Int
  ) extends CoordinatorAnswer

  case object CoordinateReadConsistencyCoordinatorCommand extends CoordinatorCommand

  case object CoordinateWriteConsistencyCoordinatorCommand extends CoordinatorCommand

  case class CoordinateReadConsistencyCoordinatorEvent(
    token:           MeteoraToken,
    tuple:           MeteoraTuple,
    readConsistency: Int,
    limit:           Int
  ) extends CoordinatorEvent

  case class CoordinateWriteConsistencyCoordinatorEvent(
    token:            MeteoraToken,
    tuple:            MeteoraTuple,
    writeConsistency: Int,
    limit:            Int
  ) extends CoordinatorCommand
}
