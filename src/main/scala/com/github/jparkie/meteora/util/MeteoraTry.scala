package com.github.jparkie.meteora.util

import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }

object MeteoraTry {
  def apply[T](r: => T): Try[T] = {
    try Success(r) catch {
      case NonFatal(e) => Failure(MeteoraException(e))
    }
  }
}
