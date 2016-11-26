package com.github.jparkie.meteora

import akka.actor.ActorSystem
import akka.cluster.Cluster
import akka.event.{ Logging, LoggingAdapter }
import akka.http.scaladsl.Http
import akka.stream.{ ActorMaterializer, Materializer }
import com.github.jparkie.meteora.cluster.ClusterActor
import com.github.jparkie.meteora.coordinator.CoordinatorActor
import com.github.jparkie.meteora.core.MeteoraTokenRangeRing
import com.github.jparkie.meteora.repair.RepairActor
import com.github.jparkie.meteora.rest.RestService
import com.github.jparkie.meteora.storage.StorageActor
import com.github.jparkie.meteora.util.MeteoraConfig
import com.typesafe.config.{ Config, ConfigFactory }

import scala.concurrent.duration._
import scala.concurrent.{ Await, ExecutionContext }

object MeteoraServer extends App with RestService {
  override implicit val config: Config = ConfigFactory.load()
  override implicit val meteoraConfig: MeteoraConfig = MeteoraConfig(config)
  override implicit val system: ActorSystem = ActorSystem("Meteora", config)
  override implicit val executionContext: ExecutionContext = system.dispatcher
  override implicit val materializer: Materializer = ActorMaterializer()

  override val logger: LoggingAdapter = Logging(system, getClass)

  val cluster = Cluster(system)
  val selfAddress = cluster.selfAddress
  cluster.registerOnMemberUp {
    val tokenRangeRing = MeteoraTokenRangeRing(meteoraConfig.numOfVirtualNodes)
    val clusterActorRef = ClusterActor.actorOf(system, meteoraConfig, tokenRangeRing)
    val storageActorRef = StorageActor.rocksDBActorOf(system, meteoraConfig, tokenRangeRing)
    val repairActorRef = RepairActor.actorOf(system, meteoraConfig, selfAddress, clusterActorRef, storageActorRef)
    val coordinatorActorRef = CoordinatorActor.actorOf(system, meteoraConfig, selfAddress, clusterActorRef)
    val actualRoutes = routes(coordinatorActorRef, repairActorRef)
    Http().bindAndHandle(actualRoutes, meteoraConfig.restInterface, meteoraConfig.restPort)
    logger.info("Initialized Meteora.")
  }
  sys.addShutdownHook {
    if (!system.whenTerminated.isCompleted) {
      cluster.down(cluster.selfAddress)
      try {
        Await.result(system.terminate(), 10 seconds)
      } catch {
        case error: Exception => System.exit(-1)
      }
    }
  }
}
