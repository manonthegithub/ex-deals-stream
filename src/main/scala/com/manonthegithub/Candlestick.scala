/*
 * Copyright 2017 Infotecs. All rights reserved.
 */
package com.manonthegithub

import java.time.Instant

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.scaladsl.Flow
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration

object Candlestick {

  def createOneMin(data: DealInfo): CandlestickOneMinute =
    CandlestickOneMinute(
      data.ticker,
      data.timestamp,
      data.timestamp,
      data.price,
      data.price,
      data.price,
      data.price,
      data.size)

  def flowOfOneMin = Flow.fromGraph(new CandleAggregatorFlow).log("Candle")

  def intervalStartForTimestamp(timestamp: Instant, interval: FiniteDuration, startOfCountdown: Long): Instant = {
    val millisMultiplier = (timestamp.toEpochMilli - startOfCountdown) / interval.toMillis
    val millis = startOfCountdown + (millisMultiplier * interval.toMillis)
    Instant.ofEpochMilli(millis)
  }


  class CandleAggregatorFlow extends GraphStage[FlowShape[TimestampedElement, CandlestickOneMinute]]{

    val in: Inlet[TimestampedElement] = Inlet("InDeals")
    val out: Outlet[CandlestickOneMinute] = Outlet("OutCandles")

    @scala.throws[Exception](classOf[Exception])
    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

      private val interval = CandlestickOneMinute.Interval
      private val start = CandlestickOneMinute.CountdownStartMilli

      private var timeDiff: Long = 0

      private val candlesticks = mutable.Map.empty[String, CandlestickOneMinute]
      private val outputQueue = mutable.Queue.empty[CandlestickOneMinute]

      override def preStart(): Unit = {
        pull(in)
      }

      setHandler(in, new InHandler {
        @scala.throws[Exception](classOf[Exception])
        override def onPush() = {
          val elem = grab(in)
//          println(s"Inside: $elem")
//          println(s"sticks $candlesticks")
          elem.element match {
            case deal: DealInfo =>
              timeDiff = elem.timestampMillis - deal.timestamp.toEpochMilli


              val currentCandle = candlesticks.get(deal.ticker)
              currentCandle match {
                case Some(c) =>
                  if (c.timestamp == dealInterval(deal.timestamp)) {
                    candlesticks.update(c.ticker, c.merge(deal))
                    pull(in)
                  } else {
                    candlesticks foreach (e => outputQueue.enqueue(e._2))
                    candlesticks.clear()
                    if(outputQueue.nonEmpty) {
                      push(out, outputQueue.dequeue())
                    }else{
                      pull(in)
                    }
                  }
                case None =>
                  if (candlesticks.nonEmpty && dealInterval(deal.timestamp) != candlesticks.head._2.timestamp) {
                    candlesticks foreach (e => outputQueue.enqueue(e._2))
                    candlesticks.clear()

                    candlesticks.update(deal.ticker, Candlestick.createOneMin(deal))

                    if(outputQueue.nonEmpty) {
                      push(out, outputQueue.dequeue())
                    }else{
                      pull(in)
                    }
                  }else{
                    candlesticks.update(deal.ticker, Candlestick.createOneMin(deal))
                    pull(in)
                  }
              }
            case Tick =>
              if (candlesticks.nonEmpty && dealInterval(Instant.ofEpochMilli(elem.timestampMillis - timeDiff)) != candlesticks.head._2.timestamp) {
                candlesticks foreach (e => outputQueue.enqueue(e._2))
                candlesticks.clear()
                if(outputQueue.nonEmpty) {
                  push(out, outputQueue.dequeue())
                }else{
                  pull(in)
                }
              }else{
                pull(in)
              }
          }
        }
      })

      setHandler(out, new OutHandler {
        @scala.throws[Exception](classOf[Exception])
        override def onPull() = {
//          println("pull")
//          println(s"sticks $candlesticks")
          if(outputQueue.nonEmpty){
            push(out, outputQueue.dequeue())
          }else{
            if(!hasBeenPulled(in)){
              pull(in)
            }
          }
        }
      })

      def dealInterval(ts: Instant) = Candlestick.intervalStartForTimestamp(ts, interval, start)
    }

    override def shape: FlowShape[TimestampedElement, CandlestickOneMinute] = FlowShape(in, out)
  }

}

trait Candlestick {

  import Candlestick._

  def ticker: String

  def openTimestamp: Instant

  def closeTimestamp: Instant

  def open: Double

  def high: Double

  def low: Double

  def close: Double

  def volume: Int

  //время начала интервала свечи
  final def timestamp: Instant = intervalStartForTimestamp(openTimestamp, interval, startingPointForCountdownMillis)

  def interval: FiniteDuration

  def startingPointForCountdownMillis: Long = 0
}

object CandlestickOneMinute {

  import scala.concurrent.duration._

  val Interval = 1 minute
  val CountdownStartMilli: Long = 0
}

case class CandlestickOneMinute(ticker: String,
                                openTimestamp: Instant,
                                closeTimestamp: Instant,
                                open: Double,
                                high: Double,
                                low: Double,
                                close: Double,
                                volume: Int) extends Candlestick {

  import CandlestickOneMinute._
  import scala.concurrent.duration._
  import scala.math._

  def merge(data: DealInfo): CandlestickOneMinute =
    if (this.ticker == data.ticker) {
      val distance = data.timestamp.toEpochMilli - this.timestamp.toEpochMilli
      if (distance >= 0 && distance.millis < interval) {
        val (newOpenTimestamp, newCloseTimestamp, newOpen, newClose) = if (data.timestamp.compareTo(openTimestamp) >= 0) {
          (openTimestamp, data.timestamp, open, data.price)
        } else {
          (data.timestamp, openTimestamp, data.price, close)
        }
        CandlestickOneMinute(
          ticker,
          newOpenTimestamp,
          newCloseTimestamp,
          newOpen,
          max(this.high, data.price),
          min(this.low, data.price),
          newClose,
          this.volume + data.size)
      } else {
        this
      }
    } else {
      this
    }

  def merge(other: CandlestickOneMinute): CandlestickOneMinute =
    if (this.ticker == other.ticker) {
      if (other.timestamp.toEpochMilli == this.timestamp.toEpochMilli) {
        val (newOpenTimestamp, newOpen) =
          if (other.openTimestamp.compareTo(openTimestamp) >= 0) (openTimestamp, open)
          else (other.openTimestamp, other.open)
        val (newCloseTimestamp, newClose) =
          if (other.closeTimestamp.compareTo(closeTimestamp) <= 0) (closeTimestamp, close)
          else (other.closeTimestamp, other.close)
        CandlestickOneMinute(
          ticker,
          newOpenTimestamp,
          newCloseTimestamp,
          newOpen,
          max(this.high, other.high),
          min(this.low, other.low),
          newClose,
          this.volume + other.volume)
      } else {
        this
      }
    } else {
      this
    }

  def interval: FiniteDuration = Interval

  override def startingPointForCountdownMillis: Long = CountdownStartMilli
}