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

class CoordinatorWriteActor(
  val meteoraConfig:    MeteoraConfig,
  val writeConsistency: Int,
  val limit:            Int
) extends Actor with ActorLogging {
  var valueState: Option[SetTupleStorageEvent] = None

  @scala.throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    super.preStart()
    val writeConsistencyTimeout = meteoraConfig.coordinatorWriteConsistencyTimeout
    context.setReceiveTimeout(writeConsistencyTimeout)
  }

  @scala.throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    context.setReceiveTimeout(Duration.Undefined)
    super.postStop()
  }

  override def receive: Receive = receiveCoordinateCommand.orElse(receiveTimeout)

  def receiveCoordinateCommand: Receive = LoggingReceive {
    case CoordinateWriteConsistencyCoordinatorCommand =>
      val captureSender = sender()
      context.become(receiveValue(captureSender, 0, 0))
  }

  def receiveTimeout: Receive = LoggingReceive {
    case ReceiveTimeout =>
      context.stop(self)
  }

  def receiveValue(originalSender: ActorRef, successCounter: Int, failureCounter: Int): Receive = LoggingReceive {
    case value: SetTupleStorageEvent =>
      valueState = Some(value)
      verifyConsistency(originalSender, successCounter + 1, failureCounter)
    case Status.Failure(error) =>
      log.error(error, s"CoordinatorWriteActor received an error from sender ${sender()}.")
      verifyConsistency(originalSender, successCounter, failureCounter + 1)
  }

  private def verifyConsistency(originalSender: ActorRef, successCounter: Int, failureCounter: Int): Unit = {
    val possibleCounter = limit - failureCounter
    if (successCounter >= writeConsistency) {
      val correctValue = valueState.get
      val coordinateWriteConsistencyCoordinatorEvent = CoordinateWriteConsistencyCoordinatorEvent(
        correctValue.token,
        correctValue.tuple,
        writeConsistency,
        limit
      )
      originalSender ! coordinateWriteConsistencyCoordinatorEvent
      context.stop(self)
    } else if (possibleCounter < writeConsistency) {
      val error = MeteoraException(s"Failed to achieve the required writeConsistency: $writeConsistency")
      originalSender ! Status.Failure(error)
      context.stop(self)
    } else {
      context.become(receiveValue(originalSender, successCounter, failureCounter).orElse(receiveTimeout))
    }
  }
}

object CoordinatorWriteActor {
  def props(meteoraConfig: MeteoraConfig, writeConsistency: Int, limit: Int): Props = Props {
    new CoordinatorWriteActor(meteoraConfig, writeConsistency, limit)
  }

  def actorOf(
    actorRefFactory:  ActorRefFactory,
    meteoraConfig:    MeteoraConfig,
    writeConsistency: Int,
    limit:            Int
  ): ActorRef = {
    actorRefFactory.actorOf(props(meteoraConfig, writeConsistency, limit))
  }

  def coordinateWriteConsistency(
    actorRefFactory:  ActorRefFactory,
    meteoraConfig:    MeteoraConfig,
    writeConsistency: Int,
    futures:          TraversableOnce[Future[SetTupleStorageEvent]]
  )(implicit executionContext: ExecutionContext): Future[CoordinateWriteConsistencyCoordinatorEvent] = {
    implicit val writeConsistencyTimeout = Timeout(meteoraConfig.coordinatorWriteConsistencyTimeout)
    val limit = futures.size
    if (writeConsistency < 1) {
      val error = new IllegalArgumentException("writeConsistency must be >= 1")
      return Future.failed(error)
    }
    if (limit < 1) {
      val error = new IllegalArgumentException("limit must be >= 1")
      return Future.failed(error)
    }
    if (writeConsistency > limit) {
      val error = new IllegalArgumentException("writeConsistency must be <= limit")
      return Future.failed(error)
    }
    val coordinatorWriteActorRef = actorOf(actorRefFactory, meteoraConfig, writeConsistency, limit)
    val coordinateFuture = ask(coordinatorWriteActorRef, CoordinateWriteConsistencyCoordinatorCommand)
      .mapTo[CoordinateWriteConsistencyCoordinatorEvent]
    futures.foreach(future => future.pipeTo(coordinatorWriteActorRef))
    coordinateFuture
  }
}