package com.github.jparkie.meteora.storage

import akka.NotUsed
import akka.actor.{ Actor, ActorLogging, ActorRef, ActorRefFactory, Props, ReceiveTimeout, Status }
import akka.event.LoggingReceive
import akka.stream.scaladsl.Sink
import com.github.jparkie.meteora.core.MeteoraTokenRange
import com.github.jparkie.meteora.storage.StorageMessages._
import com.github.jparkie.meteora.util.MeteoraConfig

import scala.concurrent.duration.Duration

class StorageSubscribeTokenRangeStreamActor(
  meteoraConfig: MeteoraConfig,
  store:         Store,
  tokenRange:    MeteoraTokenRange
) extends Actor with ActorLogging {
  import store.executionContext

  @scala.throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    super.preStart()
    val storageSubscribeTimeout = meteoraConfig.storageSubscribeTimeout
    context.setReceiveTimeout(storageSubscribeTimeout)
  }

  @scala.throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    context.setReceiveTimeout(Duration.Undefined)
    super.postStop()
  }

  override def receive: Receive = receiveStorageMessage.orElse(receiveTimeout)

  def receiveStorageMessage: Receive = LoggingReceive {
    case PublishTokenRangeStreamOnInitStorageMessage =>
      val captureSender = sender()
      captureSender ! PublishTokenRangeStreamAckStorageMessage
    case PublishTokenRangeStreamTupleStorageMessage(token, tuple) =>
      val captureSender = sender()
      val setFuture = store.set(token, tuple)
      setFuture.onComplete(_ => captureSender ! PublishTokenRangeStreamAckStorageMessage)
    case PublishTokenRangeStreamOnCompleteStorageMessage =>
      context.stop(self)
    case Status.Failure(error) =>
      context.stop(self)
  }

  def receiveTimeout: Receive = LoggingReceive {
    case ReceiveTimeout =>
      context.stop(self)
  }
}

object StorageSubscribeTokenRangeStreamActor {
  def props(meteoraConfig: MeteoraConfig, store: Store, tokenRange: MeteoraTokenRange): Props = Props {
    new StorageSubscribeTokenRangeStreamActor(meteoraConfig, store, tokenRange)
  }

  def actorOf(
    actorRefFactory: ActorRefFactory,
    meteoraConfig:   MeteoraConfig,
    store:           Store,
    tokenRange:      MeteoraTokenRange
  ): ActorRef = {
    val childProps = props(meteoraConfig, store, tokenRange)
    actorRefFactory.actorOf(childProps)
  }

  def sinkWithAck(actorRef: ActorRef): Sink[PublishTokenRangeStreamTupleStorageMessage, NotUsed] = {
    Sink.actorRefWithAck[PublishTokenRangeStreamTupleStorageMessage](
      actorRef,
      PublishTokenRangeStreamOnInitStorageMessage,
      PublishTokenRangeStreamAckStorageMessage,
      PublishTokenRangeStreamOnCompleteStorageMessage
    )
  }
}
