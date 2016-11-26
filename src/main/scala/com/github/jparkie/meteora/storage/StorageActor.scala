package com.github.jparkie.meteora.storage

import akka.actor.{ Actor, ActorLogging, ActorRef, ActorRefFactory, ActorSelection, Address, Props, RootActorPath }
import akka.event.LoggingReceive
import akka.pattern.{ Backoff, BackoffSupervisor, pipe }
import com.github.jparkie.meteora.core.MeteoraTokenRangeRing
import com.github.jparkie.meteora.repair.merkle.MerkleTreeBuilder
import com.github.jparkie.meteora.storage.StorageMessages._
import com.github.jparkie.meteora.storage.memory.MemoryStore
import com.github.jparkie.meteora.storage.rocksdb.RocksDBStore
import com.github.jparkie.meteora.util.MeteoraConfig

import scala.concurrent.{ ExecutionContext, Future, Promise }

trait StorageActor extends Actor with ActorLogging { this: Store with StoreBase =>
  override implicit val executionContext: ExecutionContext = {
    context.system.dispatchers.lookup(meteoraConfig.storeDispatcherId)
  }

  @scala.throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    super.preStart()
    open().get
  }

  @scala.throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    close().get
    super.postStop()
  }

  override def receive: Receive = receiveStorageCommand
    .orElse(receiveStorageQuery)

  def receiveStorageCommand: Receive = LoggingReceive {
    case command @ SetTupleStorageCommand(token, tuple) =>
      val captureSender = sender()
      val setTupleStorageEventFuture = handleSetTupleStorageCommand(command)
      setTupleStorageEventFuture.pipeTo(captureSender)
    case command @ RepairTupleStorageCommand(token, tuple) =>
      val captureSender = sender()
      val repairTupleStorageEventFuture = handleRepairTupleStorageCommand(command)
      repairTupleStorageEventFuture.pipeTo(captureSender)
    case command @ HashTokenRangeStorageCommand(tokenRange, numOfPartitions) =>
      val captureSender = sender()
      val hashTokenRangeStorageEventFuture = handleHashTokenRangeStorageCommand(command)
      hashTokenRangeStorageEventFuture.pipeTo(captureSender)
    case command @ SubscribeTokenRangeStreamStorageCommand(tokenRange) =>
      val captureSender = sender()
      val subscribeTokenRangeStreamStorageEventFuture = handleSubscribeTokenRangeStreamStorageCommand(command)
      subscribeTokenRangeStreamStorageEventFuture.pipeTo(captureSender)
    case command @ PublishTokenRangeStreamStorageCommand(tokenRange, subscriberActorRef) =>
      val captureSender = sender()
      val publishTokenRangeStreamStorageEventFuture = handlePublishTokenRangeStreamStorageCommand(command)
      publishTokenRangeStreamStorageEventFuture.pipeTo(captureSender)
  }

  def receiveStorageQuery: Receive = LoggingReceive {
    case query @ GetTupleStorageQuery(token) =>
      val captureSender = sender()
      val getTupleStorageAnswerFuture = handleGetTupleStorageQuery(query)
      getTupleStorageAnswerFuture.pipeTo(captureSender)
  }

  private def handleSetTupleStorageCommand(
    command: SetTupleStorageCommand
  ): Future[SetTupleStorageEvent] = {
    val SetTupleStorageCommand(token, tuple) = command
    val setFuture = set(token, tuple)
    val setEventFuture = setFuture.map(_ => SetTupleStorageEvent(token, tuple))
    setEventFuture
  }

  private def handleRepairTupleStorageCommand(
    command: RepairTupleStorageCommand
  ): Future[RepairTupleStorageEvent] = {
    val RepairTupleStorageCommand(token, tuple) = command
    val repairFuture = set(token, tuple)
    val repairEventFuture = repairFuture.map(_ => RepairTupleStorageEvent(token, tuple))
    repairEventFuture
  }

  private def handleHashTokenRangeStorageCommand(
    command: HashTokenRangeStorageCommand
  ): Future[HashTokenRangeStorageEvent] = {
    val HashTokenRangeStorageCommand(tokenRange, numOfPartitions) = command
    val merkleTreeBuilder = new MerkleTreeBuilder(tokenRange, numOfPartitions)
    val hashTokenRangeStorageEventFuture = entries(tokenRange)
      .runForeach(merkleTreeBuilder.addEntry)
      .map(_ => merkleTreeBuilder.build())
      .map(merkleTree => HashTokenRangeStorageEvent(tokenRange, numOfPartitions, merkleTree))
    hashTokenRangeStorageEventFuture
  }

  private def handleSubscribeTokenRangeStreamStorageCommand(
    command: SubscribeTokenRangeStreamStorageCommand
  ): Future[SubscribeTokenRangeStreamStorageEvent] = {
    val SubscribeTokenRangeStreamStorageCommand(tokenRange) = command
    val subscriberActorRef = StorageSubscribeTokenRangeStreamActor
      .actorOf(context, meteoraConfig, this, tokenRange)
    val subscribeTokenRangeStreamStorageEventFuture = Future
      .successful(SubscribeTokenRangeStreamStorageEvent(tokenRange, subscriberActorRef))
    subscribeTokenRangeStreamStorageEventFuture
  }

  private def handlePublishTokenRangeStreamStorageCommand(
    command: PublishTokenRangeStreamStorageCommand
  ): Future[PublishTokenRangeStreamStorageEvent] = {
    val PublishTokenRangeStreamStorageCommand(tokenRange, subscriberActorRef) = command
    val event = PublishTokenRangeStreamStorageEvent(tokenRange, subscriberActorRef)
    val eventPromise = Promise[PublishTokenRangeStreamStorageEvent]
    val sinkWithAck = StorageSubscribeTokenRangeStreamActor.sinkWithAck(subscriberActorRef)
    entries(tokenRange)
      .map(currentEntry => PublishTokenRangeStreamTupleStorageMessage(currentEntry.token, currentEntry.tuple))
      .watchTermination()((_, doneFuture) => doneFuture.onComplete(_ => eventPromise.success(event)))
      .runWith(sinkWithAck)
    eventPromise.future
  }

  private def handleGetTupleStorageQuery(query: GetTupleStorageQuery): Future[GetTupleStorageAnswer] = {
    val GetTupleStorageQuery(token) = query
    val getFuture = get(token)
    val getTupleAnswerStorageFuture = getFuture.map(entry => GetTupleStorageAnswer(entry.token, entry.tuple))
    getTupleAnswerStorageFuture
  }
}

object StorageActor {
  val ActorSupervisor = "storageSupervisor"
  val ActorPrefix = "storageActor"

  def memoryProps(meteoraConfig: MeteoraConfig, tokenRangeRing: MeteoraTokenRangeRing): Props = Props {
    val _meteoraConfig = meteoraConfig
    val _tokenRangeRing = tokenRangeRing
    new StorageActor with MemoryStore {
      override def meteoraConfig: MeteoraConfig = _meteoraConfig

      override def tokenRangeRing: MeteoraTokenRangeRing = _tokenRangeRing
    }
  }

  def rocksDBProps(meteoraConfig: MeteoraConfig, tokenRangeRing: MeteoraTokenRangeRing): Props = Props {
    val _meteoraConfig = meteoraConfig
    val _tokenRangeRing = tokenRangeRing
    new StorageActor with RocksDBStore {
      override def meteoraConfig: MeteoraConfig = _meteoraConfig

      override def tokenRangeRing: MeteoraTokenRangeRing = _tokenRangeRing
    }
  }

  def memoryActorOf(
    actorRefFactory: ActorRefFactory,
    meteoraConfig:   MeteoraConfig,
    tokenRangeRing:  MeteoraTokenRangeRing
  ): ActorRef = {
    val backOffOnStop = Backoff.onStop(
      childProps = memoryProps(meteoraConfig, tokenRangeRing),
      childName = ActorPrefix,
      minBackoff = meteoraConfig.supervisorMinBackoff,
      maxBackoff = meteoraConfig.supervisorMaxBackoff,
      randomFactor = meteoraConfig.supervisorRandomFactor
    )
    val supervisorProps = BackoffSupervisor.props(backOffOnStop)
    actorRefFactory.actorOf(supervisorProps, ActorSupervisor)
  }

  def rocksDBActorOf(
    actorRefFactory: ActorRefFactory,
    meteoraConfig:   MeteoraConfig,
    tokenRangeRing:  MeteoraTokenRangeRing
  ): ActorRef = {
    val backOffOnStop = Backoff.onStop(
      childProps = rocksDBProps(meteoraConfig, tokenRangeRing),
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