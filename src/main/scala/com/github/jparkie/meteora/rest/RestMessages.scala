package com.github.jparkie.meteora.rest

object RestMessages {
  sealed trait RestMessage extends Serializable

  sealed trait RestField extends RestMessage

  sealed trait RestRequest extends RestMessage

  sealed trait RestResponse extends RestMessage

  case class TupleRestBody(
    key:   String,
    value: String
  ) extends RestField

  case class SetTupleRestRequest(
    key:              String,
    value:            String,
    writeConsistency: Int
  ) extends RestRequest

  case class GetTupleRestRequest(
    key:             String,
    readConsistency: Int
  ) extends RestRequest

  case class SetTupleSuccessRestResponse(
    key:              String,
    value:            String,
    timestamp:        Long,
    writeConsistency: Int
  ) extends RestResponse

  case class SetTupleFailureRestResponse(
    errorMessage: String
  ) extends RestResponse

  case class GetTupleSuccessRestResponse(
    key:             String,
    value:           String,
    timestamp:       Long,
    readConsistency: Int
  ) extends RestResponse

  case class GetTupleFailureRestResponse(
    errorMessage: String
  ) extends RestResponse

  case class RepairNodeSuccessRestResponse(
    repairId: String
  ) extends RestResponse

  case class RepairNodeFailureRestResponse(
    errorMessage: String
  ) extends RestResponse
}
