package com.github.jparkie.meteora.rest

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import com.github.jparkie.meteora.rest.RestMessages._
import spray.json._

trait RestProtocols extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val tupleRestBodyFormat: RootJsonFormat[TupleRestBody] = {
    jsonFormat2(TupleRestBody)
  }
  implicit val setTupleRestRequestFormat: RootJsonFormat[SetTupleRestRequest] = {
    jsonFormat3(SetTupleRestRequest)
  }
  implicit val getTupleRestRequestFormat: RootJsonFormat[GetTupleRestRequest] = {
    jsonFormat2(GetTupleRestRequest)
  }
  implicit val setTupleSuccessRestResponseFormat: RootJsonFormat[SetTupleSuccessRestResponse] = {
    jsonFormat4(SetTupleSuccessRestResponse)
  }
  implicit val setTupleFailureRestResponseFormat: RootJsonFormat[SetTupleFailureRestResponse] = {
    jsonFormat1(SetTupleFailureRestResponse)
  }
  implicit val getTupleSuccessRestResponseFormat: RootJsonFormat[GetTupleSuccessRestResponse] = {
    jsonFormat4(GetTupleSuccessRestResponse)
  }
  implicit val getTupleFailureRestResponseFormat: RootJsonFormat[GetTupleFailureRestResponse] = {
    jsonFormat1(GetTupleFailureRestResponse)
  }
  implicit val repairNodeSuccessRestResponseFormat: RootJsonFormat[RepairNodeSuccessRestResponse] = {
    jsonFormat1(RepairNodeSuccessRestResponse)
  }
  implicit val repairNodeFailureRestResponseFormat: RootJsonFormat[RepairNodeFailureRestResponse] = {
    jsonFormat1(RepairNodeFailureRestResponse)
  }
}
