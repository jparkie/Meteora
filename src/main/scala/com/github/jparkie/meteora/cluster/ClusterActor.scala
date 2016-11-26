package com.github.jparkie.meteora.cluster

import akka.actor.{ Actor, ActorLogging, ActorRef, ActorRefFactory, ActorSelection, Address, Props, RootActorPath }
import akka.cluster.ClusterEvent._
import akka.cluster.{ Cluster, Member, MemberStatus }
import akka.event.LoggingReceive
import akka.pattern.{ Backoff, BackoffSupervisor }
import com.github.jparkie.meteora.cluster.ClusterMessages._
import com.github.jparkie.meteora.core.{ MeteoraNodeRing, MeteoraTokenRangeRing }
import com.github.jparkie.meteora.util.MeteoraConfig

import scala.collection.immutable

class ClusterActor(
  val meteoraConfig:  MeteoraConfig,
  val tokenRangeRing: MeteoraTokenRangeRing
) extends Actor with ActorLogging {
  val cluster = Cluster(context.system)

  var membersState: immutable.SortedMap[Member, MemberStatus] = immutable.SortedMap.empty[Member, MemberStatus]
  var nodesUnreachableState: Set[Address] = Set.empty[Address]
  var nodeRing: MeteoraNodeRing = createNodeRing()

  @scala.throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    super.preStart()
    cluster.subscribe(self, classOf[MemberEvent], classOf[ReachabilityEvent])
  }

  @scala.throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    cluster.unsubscribe(self)
    super.postStop()
  }

  override def receive: Receive = receiveClusterQuery
    .orElse(receiveCurrentClusterState)
    .orElse(receiveMemberEvents)
    .orElse(receiveReachabilityEvent)

  def receiveClusterQuery: Receive = LoggingReceive {
    case query @ GetTokenRangeForTokenClusterQuery(token) =>
      val captureSender = sender()
      val tokenRange = tokenRangeRing.tokenRangeFor(token)
      captureSender ! GetTokenRangeForTokenClusterAnswer(tokenRange)
    case query @ GetTokenRangesForNodeClusterQuery(node) =>
      val captureSender = sender()
      val tokenRanges = nodeRing.tokenRangesFor(node)
      captureSender ! GetTokenRangesForNodeClusterAnswer(tokenRanges)
    case query @ GetNodeForTokenClusterQuery(token) =>
      val captureSender = sender()
      val node = nodeRing.nodeFor(token)
      captureSender ! GetNodeForTokenClusterAnswer(node)
    case query @ GetNodeForTokenRangeClusterQuery(tokenRange) =>
      val captureSender = sender()
      val node = nodeRing.nodeFor(tokenRange)
      captureSender ! GetNodeForTokenRangeClusterAnswer(node)
    case query @ GetPreferenceListForTokenClusterQuery(token, numOfReplicas) =>
      val captureSender = sender()
      val nodes = nodeRing.preferenceListFor(token, numOfReplicas)
      captureSender ! GetPreferenceListForTokenClusterAnswer(nodes)
    case query @ GetPreferenceListForTokenRangeClusterQuery(tokenRange, numOfReplicas) =>
      val captureSender = sender()
      val nodes = nodeRing.preferenceListFor(tokenRange, numOfReplicas)
      captureSender ! GetPreferenceListForTokenRangeClusterAnswer(nodes)
    case query @ GetReachablePreferenceListForTokenClusterQuery(token, numOfReplicas) =>
      val captureSender = sender()
      val nodes = nodeRing.preferenceListFor(token, numOfReplicas)
      val reachableNodes = nodes.filterNot(node => nodesUnreachableState.contains(node))
      captureSender ! GetReachablePreferenceListForTokenClusterAnswer(reachableNodes)
    case query @ GetReachablePreferenceListForTokenRangeClusterQuery(tokenRange, numOfReplicas) =>
      val captureSender = sender()
      val nodes = nodeRing.preferenceListFor(tokenRange, numOfReplicas)
      val reachableNodes = nodes.filterNot(node => nodesUnreachableState.contains(node))
      captureSender ! GetReachablePreferenceListForTokenRangeClusterAnswer(reachableNodes)
  }

  def receiveCurrentClusterState: Receive = LoggingReceive {
    case currentClusterState @ CurrentClusterState(members, unreachable, seenBy, leader, roleLeaderMap) =>
      membersState = membersState ++ members.map(member => member -> member.status)
      nodesUnreachableState = nodesUnreachableState ++ unreachable.map(member => member.address)
      val nodeSet = membersState.keySet.map(member => member.address)
      nodeRing = MeteoraNodeRing(nodeSet, tokenRangeRing)
      log.debug(s"Initial nodeSet $nodeSet.")
  }

  def receiveMemberEvents: Receive = LoggingReceive {
    case memberEvent: MemberEvent =>
      val affectedMember = memberEvent.member
      membersState = membersState + (affectedMember -> affectedMember.status)
      memberEvent match {
        case MemberUp(member)                            =>
        // TODO: Disable Automatic Joining; Have Node Tool to Add Nodes.
        // nodeRing = nodeRing :+ member.address
        case MemberRemoved(member, MemberStatus.Exiting) =>
        // TODO: Disable Automatic Leaving; Have Node Tool to Remove Nodes.
        // nodeRing = nodeRing :- member.address
        case MemberRemoved(member, MemberStatus.Down) =>
          nodesUnreachableState = nodesUnreachableState + member.address
        case _ =>
        // Do Nothing.
      }
  }

  def receiveReachabilityEvent: Receive = LoggingReceive {
    case unreachableMember @ UnreachableMember(member) =>
      nodesUnreachableState = nodesUnreachableState + member.address
    case reachableMember @ ReachableMember(member) =>
      nodesUnreachableState = nodesUnreachableState - member.address
  }

  private def createNodeRing(): MeteoraNodeRing = {
    val initialSelfSet = Set(cluster.selfAddress)
    MeteoraNodeRing(initialSelfSet, tokenRangeRing)
  }
}

object ClusterActor {
  val ActorSupervisor = "clusterSupervisor"
  val ActorPrefix = "clusterActor"

  def props(meteoraConfig: MeteoraConfig, tokenRangeRing: MeteoraTokenRangeRing): Props = Props {
    new ClusterActor(meteoraConfig, tokenRangeRing)
  }

  def actorOf(
    actorRefFactory: ActorRefFactory,
    meteoraConfig:   MeteoraConfig,
    tokenRangeRing:  MeteoraTokenRangeRing
  ): ActorRef = {
    val backOffOnStop = Backoff.onStop(
      childProps = props(meteoraConfig, tokenRangeRing),
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