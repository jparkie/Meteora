package com.github.jparkie.meteora.repair

import java.util.UUID
import java.util.concurrent.Executors

import akka.actor.{ Actor, ActorLogging, ActorRef, ActorRefFactory, ActorSelection, Address, Props, RootActorPath }
import akka.event.LoggingReceive
import akka.pattern.{ Backoff, BackoffSupervisor, after, ask, pipe }
import akka.util.Timeout
import com.github.jparkie.meteora.cluster.ClusterMessages._
import com.github.jparkie.meteora.core.MeteoraTokenRange
import com.github.jparkie.meteora.repair.RepairMessages._
import com.github.jparkie.meteora.repair.merkle.{ MerkleTree, MerkleTreeOps }
import com.github.jparkie.meteora.storage.StorageActor
import com.github.jparkie.meteora.storage.StorageMessages._
import com.github.jparkie.meteora.util.{ MeteoraConfig, MeteoraException, MeteoraFuture, MeteoraTry }

import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.util.Try

class RepairActor(
  meteoraConfig:   MeteoraConfig,
  selfAddress:     Address,
  clusterActorRef: ActorRef,
  storageActorRef: ActorRef
) extends Actor with ActorLogging {
  import context.dispatcher

  val repairExecutionContext = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())

  var repairNodeRepairEventState: Option[RepairNodeRepairEvent] = None

  override def receive: Receive = receiveRepairCommand

  def receiveRepairCommand: Receive = LoggingReceive {
    case RepairNodeRepairCommand =>
      val captureSender = sender()
      val repairNodeRepairEventFuture = handleRepairNodeRepairCommand()
      repairNodeRepairEventFuture.pipeTo(captureSender)
  }

  private def handleRepairNodeRepairCommand(): Future[RepairNodeRepairEvent] = {
    repairNodeRepairEventState match {
      case Some(event) =>
        val error = MeteoraException(s"Failed to start a repair for node $selfAddress as it is already repairing.")
        Future.failed(error)
      case None =>
        // Updated Internal State:
        val newRepairId = UUID.randomUUID.toString
        val newEvent = RepairNodeRepairEvent(newRepairId)
        repairNodeRepairEventState = Some(newEvent)
        // Launch Repair:
        val repairFuture = repair()
        // Timeout Repair:
        val totalTimeout = Timeout(meteoraConfig.totalTimeout)
        val timeoutFuture = after(totalTimeout.duration, context.system.scheduler) {
          val error = MeteoraException(s"Repair $newRepairId timeout after $totalTimeout ms")
          Future.failed(error)
        }
        val completeFuture = Future.firstCompletedOf(Set(repairFuture, timeoutFuture))
        completeFuture.onComplete(_ => repairNodeRepairEventState = None)
        Future.successful(newEvent)
    }
  }

  /*
   * Note:
   *
   * The repair() function is far from an ideal anti-entropy node repair algorithm.
   * This function operates under a very optimistic assumption that a repair is not required
   * to bring the database to a "strong" consistency. Instead, this function aims to optimistically reduce entropy.
   * Thus, amidst failures, this function prioritizes continuing the repair at "best effort".
   *
   * Possible Improvements:
   * - There exists a foundation to make the MerkleTree-based difference procedure
   *   to recursively build new subtrees for larger, different tokenRanges until a sizable threshold is reached.
   *   This improvement would reduce the number of overlap streamed for the repair.
   * - Designing a persistent finite-state-machine to handle the repair would be ideal
   *   to recover from any transient failure.
   * - Utilizing Enterprise Integration Patterns to build a proper ProcessManager to handle this with a set of actors.
   *
   * Algorithm:
   * 1. Fetch all the token ranges of the virtual nodes owned by this node.
   * 2. Foreach virtual node token range, fetch a reachable preference list of nodes which hold a replica.
   * 3. Foreach node in the reachable preference list, establish a connection to the node.
   * 4. With the established connection, construct Merkle Trees for this node and the remote node.
   * 5. Calculate the difference between the local and the remote Merkle Trees.
   * 6. Stream the differences from the remote node to this node.
   */
  // format: OFF
  private def repair(): Future[Unit] = MeteoraFuture {
    // Settings:
    val numOfReplicas = meteoraConfig.coordinatorReplicaConsistency
    val numOfPartitions = meteoraConfig.numOfPartitions
    // Timeouts:
    val fetchTokenRangesTimeout = Timeout(meteoraConfig.fetchTokenRangesTimeout)
    val fetchReachablePreferenceListTimeout = Timeout(meteoraConfig.fetchReachablePreferenceListTimeout)
    val fetchStorageActorRefTimeout = Timeout(meteoraConfig.fetchStorageActorRefTimeout)
    val fetchMerkleTreeTimeout = Timeout(meteoraConfig.fetchMerkleTreeTimeout)
    val fetchSubscriberActorRefTimeout = Timeout(meteoraConfig.fetchSubscriberActorRefTimeout)
    val streamTokenRangeTimeout = Timeout(meteoraConfig.streamTokenRangeTimeout)
    // Repair Algorithm:
    log.debug(s"Starting repair for node $selfAddress.")
    log.debug(s"Fetching tokenRanges for node $selfAddress.")
    val tokenRangesTry = fetchTokenRanges(selfAddress)(fetchTokenRangesTimeout)
    if (tokenRangesTry.isFailure) {
      log.error(tokenRangesTry.failed.get, s"Failed to fetch tokenRanges for node $selfAddress.")
    }
    tokenRangesTry.foreach { tokenRanges =>
      log.debug(s"Fetched tokenRanges $tokenRanges for node $selfAddress.")
      tokenRanges.foreach { virtualNodeTokenRange =>
        log.debug(s"Fetching reachablePreferenceList for virtualNodeTokenRange $virtualNodeTokenRange.")
        val reachablePreferenceListTry = fetchReachablePreferenceList(virtualNodeTokenRange, numOfReplicas)(fetchReachablePreferenceListTimeout)
        if (reachablePreferenceListTry.isFailure) {
          log.error(reachablePreferenceListTry.failed.get, s"Failed to fetch reachablePreferenceList for virtualNodeTokenRange $virtualNodeTokenRange.")
        }
        reachablePreferenceListTry.foreach { reachablePreferenceList =>
          log.debug(s"Fetched reachablePreferenceList $reachablePreferenceList for virtualNodeTokenRange $virtualNodeTokenRange.")
          reachablePreferenceList.foreach { reachablePreferenceNode =>
            log.debug(s"Fetching remoteStorageActorRef for reachablePreferenceNode $reachablePreferenceNode.")
            val remoteStorageActorRefTry = fetchStorageActorRef(reachablePreferenceNode)(fetchStorageActorRefTimeout)
            if (remoteStorageActorRefTry.isFailure) {
              log.error(remoteStorageActorRefTry.failed.get, s"Failed to fetch remoteStorageActorRef for reachablePreferenceNode $reachablePreferenceNode.")
            }
            remoteStorageActorRefTry.foreach { remoteStorageActorRef =>
              log.debug(s"Fetched remoteStorageActorRef $remoteStorageActorRef for reachablePreferenceNode $reachablePreferenceNode.")
              log.debug(s"Calculating differentTokenRanges in virtualNodeTokenRange $virtualNodeTokenRange against reachablePreferenceNode $reachablePreferenceNode.")
              val differentTokenRangesTry = for {
                remoteMerkleTree <- fetchMerkleTree(remoteStorageActorRef, virtualNodeTokenRange, numOfPartitions)(fetchMerkleTreeTimeout)
                localMerkleTree <- fetchMerkleTree(storageActorRef, virtualNodeTokenRange, numOfPartitions)(fetchMerkleTreeTimeout)
              } yield {
                log.debug(s"Built Merkle Trees for virtualNodeTokenRange $virtualNodeTokenRange.")
                MerkleTreeOps.diff(localMerkleTree, remoteMerkleTree)
              }
              if (differentTokenRangesTry.isFailure) {
                log.error(differentTokenRangesTry.failed.get, s"Failed to fetch differentTokenRanges for virtualNodeTokenRange $virtualNodeTokenRange.")
              }
              differentTokenRangesTry.foreach { differentTokenRanges =>
                log.debug(s"Calculated differentTokenRanges in virtualNodeTokenRange $virtualNodeTokenRange against reachablePreferenceNode $reachablePreferenceNode.")
                differentTokenRanges.foreach { differentTokenRange =>
                  log.debug(s"Fetching subscriberActorRef for differentTokenRange $differentTokenRange.")
                  val subscriberActorRefTry = fetchSubscriberActorRef(differentTokenRange)(fetchSubscriberActorRefTimeout)
                  if (subscriberActorRefTry.isFailure) {
                    log.error(subscriberActorRefTry.failed.get, s"Failed to fetch subscriberActorRef for differentTokenRange $differentTokenRange.")
                  }
                  subscriberActorRefTry.foreach { subscriberActorRef =>
                    log.debug(s"Streaming differentTokenRange $differentTokenRange from reachablePreferenceNode $reachablePreferenceNode.")
                    val streamTokenRangeTry = streamTokenRange(remoteStorageActorRef, subscriberActorRef, differentTokenRange)(streamTokenRangeTimeout)
                    if (streamTokenRangeTry.isFailure) {
                      log.error(streamTokenRangeTry.failed.get, s"Failed to stream differentTokenRange $differentTokenRange from reachablePreferenceNode $reachablePreferenceNode.")
                    }
                    streamTokenRangeTry.foreach { _ =>
                      log.debug(s"Streamed differentTokenRanges $differentTokenRange from reachablePreferenceNode $reachablePreferenceNode.")
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    log.debug(s"Finished repairing for node $selfAddress.")
  } (repairExecutionContext)
  // format: ON

  private def fetchTokenRanges(
    node: Address
  )(implicit timeout: Timeout): Try[List[MeteoraTokenRange]] = MeteoraTry {
    val query = GetTokenRangesForNodeClusterQuery(node)
    val answerFuture = ask(clusterActorRef, query)
      .mapTo[GetTokenRangesForNodeClusterAnswer]
      .map(answer => answer.tokenRanges)
    Await.result(answerFuture, timeout.duration)
  }

  private def fetchReachablePreferenceList(
    tokenRange:    MeteoraTokenRange,
    numOfReplicas: Int
  )(implicit timeout: Timeout): Try[List[Address]] = MeteoraTry {
    val query = GetReachablePreferenceListForTokenRangeClusterQuery(tokenRange, numOfReplicas)
    val answerFuture = ask(clusterActorRef, query)
      .mapTo[GetReachablePreferenceListForTokenRangeClusterAnswer]
      .map(answer => answer.nodes)
    Await.result(answerFuture, timeout.duration)
  }

  private def fetchStorageActorRef(
    node: Address
  )(implicit timeout: Timeout): Try[ActorRef] = MeteoraTry {
    val actorRefFuture = StorageActor.actorSelection(context, node)
      .resolveOne()
    Await.result(actorRefFuture, timeout.duration)
  }

  private def fetchMerkleTree(
    actorRef:        ActorRef,
    tokenRange:      MeteoraTokenRange,
    numOfPartitions: Int
  )(implicit timeout: Timeout): Try[MerkleTree] = MeteoraTry {
    val command = HashTokenRangeStorageCommand(tokenRange, numOfPartitions)
    val eventFuture = ask(actorRef, command)
      .mapTo[HashTokenRangeStorageEvent]
      .map(event => event.merkleTree)
    Await.result(eventFuture, timeout.duration)
  }

  private def fetchSubscriberActorRef(
    tokenRange: MeteoraTokenRange
  )(implicit timeout: Timeout): Try[ActorRef] = MeteoraTry {
    val command = SubscribeTokenRangeStreamStorageCommand(tokenRange)
    val eventFuture = ask(storageActorRef, command)
      .mapTo[SubscribeTokenRangeStreamStorageEvent]
      .map(event => event.subscriberActorRef)
    Await.result(eventFuture, timeout.duration)
  }

  private def streamTokenRange(
    remoteStorageActorRef:   ActorRef,
    localSubscriberActorRef: ActorRef,
    tokenRange:              MeteoraTokenRange
  )(implicit timeout: Timeout): Try[Unit] = MeteoraTry {
    val command = PublishTokenRangeStreamStorageCommand(tokenRange, localSubscriberActorRef)
    val eventFuture = ask(remoteStorageActorRef, command)
      .mapTo[PublishTokenRangeStreamStorageEvent]
      .map(event => Unit)
    Await.result(eventFuture, timeout.duration)
  }
}

object RepairActor {
  val ActorSupervisor = "repairSupervisor"
  val ActorPrefix = "repairActor"

  def props(
    meteoraConfig:   MeteoraConfig,
    selfAddress:     Address,
    clusterActorRef: ActorRef,
    storageActorRef: ActorRef
  ): Props = Props {
    new RepairActor(meteoraConfig, selfAddress, clusterActorRef, storageActorRef)
  }

  def actorOf(
    actorRefFactory: ActorRefFactory,
    meteoraConfig:   MeteoraConfig,
    selfAddress:     Address,
    clusterActorRef: ActorRef,
    storageActorRef: ActorRef
  ): ActorRef = {
    val backOffOnStop = Backoff.onStop(
      childProps = props(meteoraConfig, selfAddress, clusterActorRef, storageActorRef),
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
