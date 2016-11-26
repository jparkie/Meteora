package com.github.jparkie.meteora.util

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.control.NonFatal

object MeteoraFuture {
  def apply[T](body: => T)(implicit executor: ExecutionContext): Future[T] = {
    Future(body).recoverWith {
      case NonFatal(e) => Future.failed(MeteoraException(e))
    }
  }
}
