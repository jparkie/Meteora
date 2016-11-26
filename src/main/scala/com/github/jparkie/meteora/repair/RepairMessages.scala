package com.github.jparkie.meteora.repair

object RepairMessages {
  sealed trait RepairMessage extends Serializable {
    val messageTimestamp: Long = System.currentTimeMillis()
  }

  sealed trait RepairCommand extends RepairMessage

  sealed trait RepairEvent extends RepairMessage

  sealed trait RepairQuery extends RepairMessage

  sealed trait RepairAnswer extends RepairMessage

  case object RepairNodeRepairCommand extends RepairCommand

  case class RepairNodeRepairEvent(
    repairId: String
  ) extends RepairEvent
}
