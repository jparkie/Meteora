package com.github.jparkie.meteora.coordinator

import akka.actor.{ Actor, ActorLogging, ActorRef, ActorRefFactory, Props, ReceiveTimeout, Status }
import akka.event.LoggingReceive
import akka.pattern.{ ask, pipe }
import akka.util.Timeout
import com.github.jparkie.meteora.coordinator.CoordinatorMessages._
import com.github.jparkie.meteora.storage.StorageMessages._
import com.github.jparkie.meteora.util.{ MeteoraConfig, MeteoraException }

import scala.concurrent.duration.Duration
import scala.concurrent.{ ExecutionContext, Future }

class CoordinatorReadActor(
  val meteoraConfig:   MeteoraConfig,
  val readConsistency: Int,
  val limit:           Int
) extends Actor with ActorLogging {
  var valueState: Option[GetTupleStorageAnswer] = None
  var replicas: Map[ActorRef, GetTupleStorageAnswer] = Map.empty[ActorRef, GetTupleStorageAnswer]

  @scala.throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    super.preStart()
    val readConsistencyTimeout = meteoraConfig.coordinatorReadConsistencyTimeout
    context.setReceiveTimeout(readConsistencyTimeout)
  }

  @scala.throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    context.setReceiveTimeout(Duration.Undefined)
    super.postStop()
  }

  override def receive: Receive = receiveCoordinateCommand.orElse(receiveTimeout)

  def receiveCoordinateCommand: Receive = LoggingReceive {
    case CoordinateReadConsistencyCoordinatorCommand =>
      val captureSender = sender()
      context.become(receiveValue(captureSender, 0, 0))
  }

  def receiveTimeout: Receive = LoggingReceive {
    case ReceiveTimeout =>
      context.stop(self)
  }

  def receiveValue(originalSender: ActorRef, successCounter: Int, failureCounter: Int): Receive = LoggingReceive {
    case value: GetTupleStorageAnswer =>
      valueState = lastWriteWins(value)
      replicas = replicas + (sender() -> value)
      verifyConsistency(originalSender, successCounter + 1, failureCounter)
    case Status.Failure(error) =>
      log.error(error, s"CoordinatorReadActor received an error from sender ${sender()}.")
      verifyConsistency(originalSender, successCounter, failureCounter + 1)
  }

  private def lastWriteWins(newValue: GetTupleStorageAnswer): Option[GetTupleStorageAnswer] = valueState match {
    case Some(currentValue) if newValue.messageTimestamp >= currentValue.messageTimestamp =>
      Some(newValue)
    case None =>
      Some(newValue)
    case _ =>
      valueState
  }

  private def readRepair(correctValue: GetTupleStorageAnswer): Unit = {
    val incorrectReplicas = replicas.collect {
      case (originalSender, originalValue) if originalValue != correctValue =>
        originalSender
    }
    val correctCommand = RepairTupleStorageCommand(correctValue.token, correctValue.tuple)
    incorrectReplicas.foreach { currentReplica =>
      currentReplica.tell(correctCommand, ActorRef.noSender)
    }
  }

  private def verifyConsistency(originalSender: ActorRef, successCounter: Int, failureCounter: Int): Unit = {
    val possibleCounter = limit - failureCounter
    if (successCounter >= readConsistency) {
      val correctValue = valueState.get
      val coordinateReadConsistencyCoordinatorEvent = CoordinateReadConsistencyCoordinatorEvent(
        correctValue.token,
        correctValue.tuple,
        readConsistency,
        limit
      )
      originalSender ! coordinateReadConsistencyCoordinatorEvent
      readRepair(correctValue)
      context.stop(self)
    } else if (possibleCounter < readConsistency) {
      val error = MeteoraException(s"Failed to achieve the required readConsistency: $readConsistency")
      originalSender ! Status.Failure(error)
      context.stop(self)
    } else {
      context.become(receiveValue(originalSender, successCounter, failureCounter).orElse(receiveTimeout))
    }
  }
}

object CoordinatorReadActor {
  def props(meteoraConfig: MeteoraConfig, readConsistency: Int, limit: Int): Props = Props {
    new CoordinatorReadActor(meteoraConfig, readConsistency, limit)
  }

  def actorOf(
    actorRefFactory: ActorRefFactory,
    meteoraConfig:   MeteoraConfig,
    readConsistency: Int,
    limit:           Int
  ): ActorRef = {
    actorRefFactory.actorOf(props(meteoraConfig, readConsistency, limit))
  }

  def coordinateReadConsistency(
    actorRefFactory: ActorRefFactory,
    meteoraConfig:   MeteoraConfig,
    readConsistency: Int,
    futures:         TraversableOnce[Future[GetTupleStorageAnswer]]
  )(implicit executionContext: ExecutionContext): Future[CoordinateReadConsistencyCoordinatorEvent] = {
    implicit val readConsistencyTimeout = Timeout(meteoraConfig.coordinatorReadConsistencyTimeout)
    val limit = futures.size
    if (readConsistency < 1) {
      val error = new IllegalArgumentException("readConsistency must be >= 1")
      return Future.failed(error)
    }
    if (limit < 1) {
      val error = new IllegalArgumentException("limit must be >= 1")
      return Future.failed(error)
    }
    if (readConsistency > limit) {
      val error = new IllegalArgumentException("readConsistency must be <= limit")
      return Future.failed(error)
    }
    val coordinatorReadActorRef = actorOf(actorRefFactory, meteoraConfig, readConsistency, limit)
    val coordinateFuture = ask(coordinatorReadActorRef, CoordinateReadConsistencyCoordinatorCommand)
      .mapTo[CoordinateReadConsistencyCoordinatorEvent]
    futures.foreach(future => future.pipeTo(coordinatorReadActorRef))
    coordinateFuture
  }
}