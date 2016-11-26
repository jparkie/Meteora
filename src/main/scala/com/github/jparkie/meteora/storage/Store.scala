package com.github.jparkie.meteora.storage

import akka.NotUsed
import akka.actor.Actor
import akka.stream.scaladsl.Source
import com.github.jparkie.meteora.core._
import com.github.jparkie.meteora.util.MeteoraConfig

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Try

trait Store extends Actor {
  implicit val executionContext: ExecutionContext

  def meteoraConfig: MeteoraConfig

  def tokenRangeRing: MeteoraTokenRangeRing

  def open(): Try[Unit]

  def close(): Try[Unit]

  def set(token: MeteoraToken, tuple: MeteoraTuple): Future[Unit]

  def get(token: MeteoraToken): Future[MeteoraEntry]

  def tokens(tokenRange: MeteoraTokenRange): Source[MeteoraToken, NotUsed]

  def entries(tokenRange: MeteoraTokenRange): Source[MeteoraEntry, NotUsed]

  def tokens(): Source[MeteoraToken, NotUsed]

  def entries(): Source[MeteoraEntry, NotUsed]

  def drop(tokenRange: MeteoraTokenRange): Future[Unit]
}