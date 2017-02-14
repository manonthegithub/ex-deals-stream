package com.manonthegithub

import java.time.Instant

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import org.scalatest.{WordSpecLike, Matchers}

import scala.util.Random

/**
  * Created by Kirill on 12/02/2017.
  */
class TestSuite extends TestKit(ActorSystem("tester")) with WordSpecLike with Matchers {

  implicit val sys = system
  implicit val mat = ActorMaterializer()

  "TenMinFlow" should {

    "return element" in {
      Source
        .repeat(Candlestick.createOneMin(DealInfo(Instant.now(), "TC", 100.5, 100)))
        .via(Candlestick.tenMinBufferOfOneMin)
        .runWith(TestSink.probe)
        .request(1)
        .expectNext()
    }

  }

  "Candles" should {
    import scala.math._

    val DefaultInitialMessageSecond: Long = 123
    val DefaultInitialMessageMilli: Long = 123
    val DefaultInstant = Instant.ofEpochSecond(DefaultInitialMessageSecond).plusMillis(DefaultInitialMessageMilli)

    val DefaultExpectedSecond: Long = 120
    val DefaultExpectedStartIntervalInstant = Instant.ofEpochSecond(DefaultExpectedSecond)


    def generateMessage(second: Long = DefaultInitialMessageSecond,
                        milli: Long = DefaultInitialMessageMilli,
                        ticker: String = Random.nextString(4),
                        price: Double = Random.nextDouble() * 150,
                        vol: Int = 1000 - Random.nextInt(300)) =
      DealInfo(Instant.ofEpochSecond(second).plusMillis(milli), ticker, price, vol)

    "init right candle" in {
      val data = generateMessage()
      val initial = Candlestick.createOneMin(data)
      initial should matchPattern {
        case CandlestickOneMinute(data.ticker, DefaultInstant, DefaultInstant, data.price, data.price, data.price, data.price, data.size) =>
      }
      initial.timestamp should be(DefaultExpectedStartIntervalInstant)
    }

    // Candlesticks

    "merge right with same instance of candle" in {
      val data = generateMessage()
      val initial = Candlestick.createOneMin(data)
      val second = Candlestick.createOneMin(data)
      val ExpectedMerge1Vol = 2 * initial.volume

      val afterMerge = initial.merge(second)
      afterMerge should matchPattern {
        case CandlestickOneMinute(data.ticker, DefaultInstant, DefaultInstant, data.price, data.price, data.price, data.price, ExpectedMerge1Vol) =>
      }
      initial.timestamp should be(DefaultExpectedStartIntervalInstant)
    }

    "skip merge with candle with different tickers" in {
      val data = generateMessage(ticker = "T1")
      val data2 = generateMessage(ticker = "T2")

      val initial = Candlestick.createOneMin(data)
      val secondCandle = Candlestick.createOneMin(data2)

      val afterMerge = initial.merge(secondCandle)
      initial should be(afterMerge)
    }

    "skip merge with candle out of interval" in {
      val data = generateMessage()
      val initial = Candlestick.createOneMin(data)

      val data2 = generateMessage(ticker = data.ticker, second = DefaultExpectedSecond - 1)
      val secondCandle = Candlestick.createOneMin(data2)
      val afterMerge = initial.merge(secondCandle)
      initial should be(afterMerge)

      val data3 = generateMessage(ticker = data.ticker, second = DefaultExpectedSecond + initial.interval.toSeconds)
      val third = Candlestick.createOneMin(data3)
      val afterMerge2 = initial.merge(third)
      initial should be(afterMerge2)
    }

    "merge with right candle in interval with right order" in {
      val data = generateMessage()
      val initial = Candlestick.createOneMin(data)

      val data2 = generateMessage(ticker = data.ticker, second = DefaultExpectedSecond + initial.interval.toSeconds - 1)
      val secondCandle = Candlestick.createOneMin(data2)

      val ExpectedMergeVol = data.size + data2.size
      val ExpectedHigh = max(data.price, data2.price)
      val ExpectedLow = min(data.price, data2.price)

      val afterMerge = initial.merge(secondCandle)
      afterMerge should matchPattern {
        case CandlestickOneMinute(data.ticker, DefaultInstant, data2.timestamp, data.price, ExpectedHigh, ExpectedLow, data2.price, ExpectedMergeVol) =>
      }
    }

    "merge with right candle in interval with wrong order" in {
      val data = generateMessage()
      val initial = Candlestick.createOneMin(data)

      val data2 = generateMessage(ticker = data.ticker, second = DefaultExpectedSecond + 1)
      val secondCandle = Candlestick.createOneMin(data2)

      val ExpectedMergeVol = data.size + data2.size
      val ExpectedHigh = max(data.price, data2.price)
      val ExpectedLow = min(data.price, data2.price)

      val afterMerge = initial.merge(secondCandle)
      afterMerge should matchPattern {
        case CandlestickOneMinute(data.ticker, data2.timestamp, DefaultInstant, data2.price, ExpectedHigh, ExpectedLow, data.price, ExpectedMergeVol) =>
      }

    }


    // Messages

    "merge right with same message instance" in {
      val data = generateMessage()
      val initial = Candlestick.createOneMin(data)
      val ExpectedMerge1Vol = 2 * initial.volume

      val afterMerge = initial.merge(data)
      afterMerge should matchPattern {
        case CandlestickOneMinute(data.ticker, DefaultInstant, DefaultInstant, data.price, data.price, data.price, data.price, ExpectedMerge1Vol) =>
      }
      initial.timestamp should be(DefaultExpectedStartIntervalInstant)
    }

    "skip merge with message with different tickers" in {
      val data = generateMessage(ticker = "T1")
      val data2 = generateMessage(ticker = "T2")

      val initial = Candlestick.createOneMin(data)

      val afterMerge = initial.merge(data2)
      initial should be(afterMerge)
    }

    "skip merge with message out of interval" in {
      val data = generateMessage()
      val initial = Candlestick.createOneMin(data)

      val data2 = generateMessage(ticker = data.ticker, second = DefaultExpectedSecond - 1)
      val afterMerge = initial.merge(data2)
      initial should be(afterMerge)

      val data3 = generateMessage(ticker = data.ticker, second = DefaultExpectedSecond + initial.interval.toSeconds)
      val afterMerge2 = initial.merge(data3)
      initial should be(afterMerge2)
    }

    "merge with right message in interval with right order" in {
      val data = generateMessage()
      val initial = Candlestick.createOneMin(data)

      val data2 = generateMessage(ticker = data.ticker, second = DefaultExpectedSecond + initial.interval.toSeconds - 1)

      val ExpectedMergeVol = data.size + data2.size
      val ExpectedHigh = max(data.price, data2.price)
      val ExpectedLow = min(data.price, data2.price)

      val afterMerge = initial.merge(data2)
      afterMerge should matchPattern {
        case CandlestickOneMinute(data.ticker, DefaultInstant, data2.timestamp, data.price, ExpectedHigh, ExpectedLow, data2.price, ExpectedMergeVol) =>
      }
    }

    "merge with right message in interval with wrong order" in {
      val data = generateMessage()
      val initial = Candlestick.createOneMin(data)

      val data2 = generateMessage(ticker = data.ticker, second = DefaultExpectedSecond + 1)

      val ExpectedMergeVol = data.size + data2.size
      val ExpectedHigh = max(data.price, data2.price)
      val ExpectedLow = min(data.price, data2.price)

      val afterMerge = initial.merge(data2)
      afterMerge should matchPattern {
        case CandlestickOneMinute(data.ticker, data2.timestamp, DefaultInstant, data2.price, ExpectedHigh, ExpectedLow, data.price, ExpectedMergeVol) =>
      }

    }

  }


}
