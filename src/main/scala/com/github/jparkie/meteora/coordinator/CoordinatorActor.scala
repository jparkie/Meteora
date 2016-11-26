package com.github.jparkie.meteora.coordinator

import akka.actor.{ Actor, ActorLogging, ActorRef, ActorRefFactory, ActorSelection, Address, Props, RootActorPath }
import akka.event.LoggingReceive
import akka.pattern.{ Backoff, BackoffSupervisor, after, ask, pipe }
import akka.util.Timeout
import com.github.jparkie.meteora.cluster.ClusterMessages._
import com.github.jparkie.meteora.coordinator.CoordinatorMessages._
import com.github.jparkie.meteora.core.{ MeteoraToken, MeteoraTuple }
import com.github.jparkie.meteora.storage.StorageActor
import com.github.jparkie.meteora.storage.StorageMessages._
import com.github.jparkie.meteora.util.{ MeteoraConfig, MeteoraException }

import scala.concurrent.Future

class CoordinatorActor(
  meteoraConfig:   MeteoraConfig,
  selfAddress:     Address,
  clusterActorRef: ActorRef
) extends Actor with ActorLogging {
  import CoordinatorReadActor._
  import CoordinatorWriteActor._
  import context.dispatcher

  override def receive: Receive = receiveCoordinatorCommand
    .orElse(receiveCoordinatorQuery)

  def receiveCoordinatorCommand: Receive = LoggingReceive {
    case command @ SetTupleCoordinatorCommand(key, value, timestamp, writeConsistency) =>
      val captureSender = sender()
      val resultFuture = handleSetTupleCoordinatorCommand(command)
      resultFuture.pipeTo(captureSender)
  }

  def receiveCoordinatorQuery: Receive = LoggingReceive {
    case query @ GetTupleCoordinatorQuery(key, readConsistency) =>
      val captureSender = sender()
      val resultFuture = handleGetTupleCoordinatorQuery(query)
      resultFuture.pipeTo(captureSender)
  }

  private def handleSetTupleCoordinatorCommand(
    command: SetTupleCoordinatorCommand
  ): Future[SetTupleCoordinatorEvent] = {
    implicit val timeout = Timeout(meteoraConfig.coordinatorSetTimeout)
    val startTimestamp = System.currentTimeMillis()
    val SetTupleCoordinatorCommand(key, value, timestamp, writeConsistency) = command
    val token = MeteoraToken(key)
    val tuple = MeteoraTuple(key, value, timestamp)
    val numOfReplicas = meteoraConfig.coordinatorReplicaConsistency
    val clusterAnswerFuture = getClusterAnswer(token, numOfReplicas)
      .filter(answer => answer.nodes.size >= writeConsistency)
    val resultFuture = clusterAnswerFuture.flatMap {
      case result @ GetReachablePreferenceListForTokenClusterAnswer(nodes) if isSelfCoordinator(nodes) =>
        for {
          reachableNodes <- Future.successful(nodes)
          storageActorSelections = getStorageActorSelections(reachableNodes)
          storageCommand = SetTupleStorageCommand(token, tuple)
          storageEventFutures = storageActorSelections
            .map(actorSelections => ask(actorSelections, storageCommand).mapTo[SetTupleStorageEvent])
          coordinatorEvent <- coordinateWriteConsistency(context, meteoraConfig, writeConsistency, storageEventFutures)
        } yield {
          val setTupleCoordinatorEvent = SetTupleCoordinatorEvent(
            coordinatorEvent.tuple.key,
            coordinatorEvent.tuple.value,
            coordinatorEvent.tuple.timestamp,
            coordinatorEvent.writeConsistency
          )
          val endTimestamp = System.currentTimeMillis()
          val elapsedDuration = endTimestamp - startTimestamp
          log.debug(
            s"""SetTupleCoordinatorCommand Metadata:
                |  key:              ${coordinatorEvent.tuple.key}
                |  value:            ${coordinatorEvent.tuple.value}
                |  timestamp:        ${coordinatorEvent.tuple.timestamp}
                |  writeConsistency: ${coordinatorEvent.writeConsistency}
                |  limit:            ${coordinatorEvent.limit}
                |  reachableNodes:   $reachableNodes
                |  startTimestamp:   $startTimestamp
                |  endTimestamp:     $endTimestamp
                |  elapsedDuration:  $elapsedDuration
            """.stripMargin
          )
          setTupleCoordinatorEvent
        }
      case result @ GetReachablePreferenceListForTokenClusterAnswer(nodes) =>
        delegateSetTupleCoordinatorCommand(command, nodes)
    }
    timeoutFuture(resultFuture, timeout)
  }

  def handleGetTupleCoordinatorQuery(
    query: GetTupleCoordinatorQuery
  ): Future[GetTupleCoordinatorAnswer] = {
    implicit val timeout = Timeout(meteoraConfig.coordinatorGetTimeout)
    val startTimestamp = System.currentTimeMillis()
    val GetTupleCoordinatorQuery(key, readConsistency) = query
    val token = MeteoraToken(key)
    val numOfReplicas = meteoraConfig.coordinatorReplicaConsistency
    val clusterAnswerFuture = getClusterAnswer(token, numOfReplicas)
      .filter(answer => answer.nodes.size >= readConsistency)
    val resultFuture = clusterAnswerFuture.flatMap {
      case result @ GetReachablePreferenceListForTokenClusterAnswer(nodes) if isSelfCoordinator(nodes) =>
        for {
          reachableNodes <- Future.successful(nodes)
          storageActorSelections = getStorageActorSelections(reachableNodes)
          storageQuery = GetTupleStorageQuery(token)
          storageAnswerFutures = storageActorSelections
            .map(actorRef => ask(actorRef, storageQuery).mapTo[GetTupleStorageAnswer])
          coordinatorEvent <- coordinateReadConsistency(context, meteoraConfig, readConsistency, storageAnswerFutures)
        } yield {
          val getTupleCoordinatorAnswer = GetTupleCoordinatorAnswer(
            coordinatorEvent.tuple.key,
            coordinatorEvent.tuple.value,
            coordinatorEvent.tuple.timestamp,
            coordinatorEvent.readConsistency
          )
          val endTimestamp = System.currentTimeMillis()
          val elapsedDuration = endTimestamp - startTimestamp
          log.debug(
            s"""GetTupleCoordinatorQuery Metadata:
                |  key:             ${coordinatorEvent.tuple.key}
                |  value:           ${coordinatorEvent.tuple.value}
                |  timestamp:       ${coordinatorEvent.tuple.timestamp}
                |  readConsistency: ${coordinatorEvent.readConsistency}
                |  limit:           ${coordinatorEvent.limit}
                |  reachableNodes:  $reachableNodes
                |  startTimestamp:  $startTimestamp
                |  endTimestamp:    $endTimestamp
                |  elapsedDuration: $elapsedDuration
            """.stripMargin
          )
          getTupleCoordinatorAnswer
        }
      case result @ GetReachablePreferenceListForTokenClusterAnswer(nodes) =>
        delegateGetTupleCoordinatorQuery(query, nodes)
    }
    timeoutFuture(resultFuture, timeout)
  }

  private def getClusterAnswer(
    token:         MeteoraToken,
    numOfReplicas: Int
  )(implicit timeout: Timeout): Future[GetReachablePreferenceListForTokenClusterAnswer] = {
    val clusterQuery = GetReachablePreferenceListForTokenClusterQuery(token, numOfReplicas)
    ask(clusterActorRef, clusterQuery).mapTo[GetReachablePreferenceListForTokenClusterAnswer]
  }

  private def isSelfCoordinator(nodes: List[Address]): Boolean = {
    nodes.headOption.contains(selfAddress)
  }

  private def delegateSetTupleCoordinatorCommand(
    command: SetTupleCoordinatorCommand,
    nodes:   List[Address]
  )(implicit timeout: Timeout): Future[SetTupleCoordinatorEvent] = {
    val actualCoordinatorActorSelection = CoordinatorActor.actorSelection(context, nodes.headOption.get)
    ask(actualCoordinatorActorSelection, command).mapTo[SetTupleCoordinatorEvent]
  }

  private def delegateGetTupleCoordinatorQuery(
    query: GetTupleCoordinatorQuery,
    nodes: List[Address]
  )(implicit timeout: Timeout): Future[GetTupleCoordinatorAnswer] = {
    val actualCoordinatorActorSelection = CoordinatorActor.actorSelection(context, nodes.headOption.get)
    ask(actualCoordinatorActorSelection, query).mapTo[GetTupleCoordinatorAnswer]
  }

  private def getStorageActorSelections(nodes: List[Address]): List[ActorSelection] = {
    nodes.map(node => StorageActor.actorSelection(context, node))
  }

  private def timeoutFuture[T](future: Future[T], timeout: Timeout): Future[T] = {
    val timeoutFuture = after(timeout.duration, context.system.scheduler) {
      val error = MeteoraException(s"Message timeout after $timeout ms")
      Future.failed(error)
    }
    Future.firstCompletedOf(Set(future, timeoutFuture))
  }
}

object CoordinatorActor {
  val ActorSupervisor = "coordinatorSupervisor"
  val ActorPrefix = "coordinatorActor"

  def props(meteoraConfig: MeteoraConfig, selfAddress: Address, clusterActorRef: ActorRef): Props = Props {
    new CoordinatorActor(meteoraConfig, selfAddress, clusterActorRef)
  }

  def actorOf(
    actorRefFactory: ActorRefFactory,
    meteoraConfig:   MeteoraConfig,
    selfAddress:     Address,
    clusterActorRef: ActorRef
  ): ActorRef = {
    val backOffOnStop = Backoff.onStop(
      childProps = props(meteoraConfig, selfAddress, clusterActorRef),
      childName = ActorPrefix,
      minBackoff = meteoraConfig.supervisorMinBackoff,
      maxBackoff = meteoraConfig.supervisorMaxBackoff,
      randomFactor = meteoraConfig.supervisorRandomFactor
    )
    val supervisorProps = BackoffSupervisor.props(backOffOnStop)
    actorRefFactory.actorOf(supervisorProps, ActorSupervisor)
  }

  def actorSelection(actorRefFactory: ActorRefFactory, node: Address): ActorSelection = {
    val actorPath = RootActorPath(node) / "user" / ActorSupervisor / ActorPrefix
    actorRefFactory.actorSelection(actorPath)
  }
}