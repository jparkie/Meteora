package com.github.jparkie.meteora.rest

import akka.actor.{ ActorRef, ActorSystem }
import akka.event.LoggingAdapter
import akka.http.scaladsl.model.{ HttpRequest, HttpResponse, StatusCodes }
import akka.http.scaladsl.server.Directives._
import akka.pattern.ask
import akka.stream.Materializer
import akka.stream.scaladsl.Flow
import akka.util.Timeout
import com.github.jparkie.meteora.coordinator.CoordinatorMessages._
import com.github.jparkie.meteora.repair.RepairMessages._
import com.github.jparkie.meteora.rest.RestMessages._
import com.github.jparkie.meteora.util.MeteoraConfig
import com.typesafe.config.Config

import scala.concurrent.ExecutionContext
import scala.util.{ Failure, Success }

trait RestService extends RestProtocols {
  implicit val config: Config
  implicit val meteoraConfig: MeteoraConfig
  implicit val system: ActorSystem
  implicit val executionContext: ExecutionContext
  implicit val materializer: Materializer

  val logger: LoggingAdapter

  // format: OFF
  def routes(coordinatorActorRef: ActorRef, repairActorRef: ActorRef): Flow[HttpRequest, HttpResponse, Any] = {
    implicit val timeout = Timeout(meteoraConfig.restTimeout)
    logRequestResult("meteora-rest") {
      pathPrefix("meteora") {
        pathPrefix("get" / Segment) { key =>
          (get & parameter("readConsistency".as[Int] ?
            meteoraConfig.coordinatorReadConsistency)) { readConsistency =>
            val restRequest = GetTupleRestRequest(key, readConsistency)
            logger.debug(s"Received $restRequest.")
            val coordinatorQuery = GetTupleCoordinatorQuery(
              restRequest.key,
              restRequest.readConsistency
            )
            val coordinatorAnswerFuture = ask(coordinatorActorRef, coordinatorQuery).mapTo[GetTupleCoordinatorAnswer]
            onComplete(coordinatorAnswerFuture) {
              case Success(GetTupleCoordinatorAnswer(getKey, getValue, getTimestamp, getReadConsistency)) =>
                val restResponse = GetTupleSuccessRestResponse(getKey, getValue, getTimestamp, getReadConsistency)
                complete(StatusCodes.OK, restResponse)
              case Failure(error) =>
                val restResponse = GetTupleFailureRestResponse(error.getMessage)
                complete(StatusCodes.InternalServerError, restResponse)
            }
          }
        } ~
        path("set") {
          (put & parameter("writeConsistency".as[Int] ?
            meteoraConfig.coordinatorWriteConsistency)) { writeConsistency =>
            entity(as[TupleRestBody]) {
              case TupleRestBody(key, value) =>
                val restRequest = SetTupleRestRequest(key, value, writeConsistency)
                logger.debug(s"Received $restRequest.")
                val timestamp = System.currentTimeMillis()
                val coordinatorCommand = SetTupleCoordinatorCommand(
                  restRequest.key,
                  restRequest.value,
                  timestamp,
                  restRequest.writeConsistency)
                val coordinatorEventFuture = ask(coordinatorActorRef, coordinatorCommand).mapTo[SetTupleCoordinatorEvent]
                onComplete(coordinatorEventFuture) {
                  case Success(SetTupleCoordinatorEvent(setKey, setValue, setTimestamp, setWriteConsistency)) =>
                    val restResponse = SetTupleSuccessRestResponse(setKey, setValue, setTimestamp, setWriteConsistency)
                    complete(StatusCodes.OK, restResponse)
                  case Failure(error) =>
                    val restResponse = SetTupleFailureRestResponse(error.getMessage)
                    complete(StatusCodes.InternalServerError, restResponse)
                }
            }
          }
        } ~
        path("repair") {
          post {
            val repairEventFuture = ask(repairActorRef, RepairNodeRepairCommand).mapTo[RepairNodeRepairEvent]
            onComplete(repairEventFuture) {
              case Success(RepairNodeRepairEvent(repairId)) =>
                val restResponse = RepairNodeSuccessRestResponse(repairId)
                complete(StatusCodes.OK, restResponse)
              case Failure(error) =>
                val restResponse = RepairNodeFailureRestResponse(error.getMessage)
                complete(StatusCodes.InternalServerError, restResponse)
            }
          }
        }
      }
    }
  }
  // format: ON
}
