package com.github.jparkie.meteora.util

final case class MeteoraException(message: String, cause: Throwable) extends RuntimeException(message, cause)
  with Serializable {
  def this() = this(null, null)
  def this(message: String) = this(message, null)
  def this(cause: Throwable) = this(null, cause)
}

object MeteoraException {
  def apply(): MeteoraException = new MeteoraException()

  def apply(message: String): MeteoraException = new MeteoraException(message)

  def apply(cause: Throwable): MeteoraException = new MeteoraException(cause)
}

