package com.manonthegithub

import java.time.Instant

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import org.scalatest.{WordSpecLike, Matchers}

/**
  * Created by Kirill on 12/02/2017.
  */
class StreamsElementsTestSuite extends TestKit(ActorSystem("tester")) with WordSpecLike with Matchers {

  implicit val sys = system
  implicit val mat = ActorMaterializer()

  import scala.concurrent.duration._
  import TestUtils._

  "TenMinFlow" should {

    "return elements and end of batch" in {
      val delay = 100 millis

      Source
        .repeat(Candlestick.createOneMin(DealInfo(Instant.now(), "TC", 100.5, 100)))
        .delay(delay)
        .via(Candlestick.tenMinBufferOfOneMin)
        .mapConcat[StreamElement](b => b)
        .runWith(TestSink.probe)
        .request(1)
        .expectNext(EndOfBatch)
        .expectNoMsg(delay + (1 milli))
        .request(2)
        .expectNextChainingPF {
          case _: CandlestickOneMinute =>
        }.expectNext(EndOfBatch)
    }

  }

  "CandlesticksFlow" should {
    import scala.collection.immutable._

    "push candles after tick with other interval and distinguish them" in {

      val timestamp = Instant.ofEpochSecond(100)
      val timeToNextCandle = (60 - timestamp.getEpochSecond % 60) seconds
      val movedTimestamp = timestamp.toEpochMilli - (CandlestickOneMinute.Interval.toMillis / 2)

      val deal = generateMessage(second = timestamp.getEpochSecond, ticker = "TCR")
      val deal2 = generateMessage(second = timestamp.getEpochSecond, ticker = "TCR2")

      Source(
        Seq(
          TimestampedElement(deal, movedTimestamp),
          TimestampedElement(deal2, movedTimestamp),
          TimestampedElement(Tick, movedTimestamp + timeToNextCandle.toMillis)
        ))
        .log("log")
        .via(Candlestick.flowOfOneMin)
        .runWith(TestSink.probe)
        .request(2)
        .expectNext(Candlestick.createOneMin(deal))
        .expectNext(Candlestick.createOneMin(deal2))

    }

    "merge candles properly" in {
      val timestamp = Instant.ofEpochSecond(100)
      val timeToNextCandle = (60 - timestamp.getEpochSecond % 60) seconds
      val movedTimestamp = timestamp.toEpochMilli - (CandlestickOneMinute.Interval.toMillis / 2)

      val deal = generateMessage(second = timestamp.getEpochSecond, ticker = "TCR", vol = 2)
      val deal2 = generateMessage(second = timestamp.getEpochSecond, ticker = "TCR", vol = 1)

      Source(
        Seq(
          TimestampedElement(deal, movedTimestamp),
          TimestampedElement(deal2, movedTimestamp),
          TimestampedElement(Tick, movedTimestamp + timeToNextCandle.toMillis)
        ))
        .log("log")
        .via(Candlestick.flowOfOneMin)
        .runWith(TestSink.probe)
        .request(1)
        .expectNext(Candlestick.createOneMin(deal).merge(deal2))

    }

    "push after comes candle from other interval" in {
      val timestamp = Instant.ofEpochSecond(100)
      val timeToNextCandle = (60 - timestamp.getEpochSecond % 60) seconds
      val movedTimestamp = timestamp.toEpochMilli - (CandlestickOneMinute.Interval.toMillis / 2)

      val deal = generateMessage(second = timestamp.getEpochSecond, ticker = "TCR", vol = 2)
      val deal2 = generateMessage(second = timestamp.getEpochSecond + timeToNextCandle.toSeconds, ticker = "TCR", vol = 1)

      Source(
        Seq(
          TimestampedElement(deal, movedTimestamp),
          TimestampedElement(deal2, movedTimestamp)
        ))
        .log("log")
        .via(Candlestick.flowOfOneMin)
        .runWith(TestSink.probe)
        .request(1)
        .expectNext(Candlestick.createOneMin(deal))

    }

  }

}
