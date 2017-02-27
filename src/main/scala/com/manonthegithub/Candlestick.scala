package com.manonthegithub

import java.time.Instant

import akka.NotUsed
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.scaladsl.Flow
import akka.stream.stage._
import scala.collection.immutable
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

  def flowOfOneMin: Flow[TimestampedElement, Candlestick, NotUsed] = Flow
    .fromGraph(new CandleAggregatorFlow(Candlestick.createOneMin, CandlestickOneMinute.Interval, CandlestickOneMinute.CountdownStartMilli))
    .log("Candle")

  def tenMinBufferOfOneMin: Flow[Candlestick, immutable.Iterable[StreamElement], NotUsed] = Flow.fromGraph(new CandlesBuffer(10))


  def intervalStartForTimestamp(timestamp: Instant, interval: FiniteDuration, startOfCountdown: Long): Instant = {
    val millisMultiplier = (timestamp.toEpochMilli - startOfCountdown) / interval.toMillis
    val millis = startOfCountdown + (millisMultiplier * interval.toMillis)
    Instant.ofEpochMilli(millis)
  }

  /**
    * Буфер хранит несколько последних свечей
    *
    * @param sizeIntervals количество свечей, которые нужно хранить
    */
  class CandlesBuffer(sizeIntervals: Int) extends GraphStage[FlowShape[Candlestick, immutable.Iterable[StreamElement]]] {

    val in = Inlet[Candlestick]("InCandles")
    val out = Outlet[immutable.Iterable[StreamElement]]("OutBatches")

    @scala.throws[Exception](classOf[Exception])
    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new TimerGraphStageLogic(shape) {

      private case class DequeAfterIntervalEnds(timestamp: Instant)

      private var buffer = immutable.Queue.empty[Candlestick]

      override def preStart(): Unit = {
        pull(in)
      }

      setHandler(in, new InHandler {
        @scala.throws[Exception](classOf[Exception])
        override def onPush(): Unit = {
          val elem = grab(in)
          val sameTimestampWithPrevious = if (buffer.nonEmpty) elem.timestamp == buffer.last.timestamp else false
          buffer :+= elem
          // при удалении полагаться на поток свечей нельзя,
          // так как он может прерываться в случае потери соединения с сервером
          if (!sameTimestampWithPrevious) {
            scheduleOnce(DequeAfterIntervalEnds(elem.timestamp), elem.interval * sizeIntervals)
          }
          pull(in)
        }
      })

      setHandler(out, new OutHandler {
        @scala.throws[Exception](classOf[Exception])
        override def onPull(): Unit = {
          val batch = buffer :+ EndOfBatch
          push(out, batch)
        }
      })

      private def dequeFirstTimestampCandles(): Unit = {
        val timestampToDeque = buffer.head.timestamp
        buffer = buffer.dropWhile(_.timestamp == timestampToDeque)
      }

      override def onTimer(timerKey: Any): Unit = timerKey match {
        case DequeAfterIntervalEnds(_) =>
          if (buffer.nonEmpty) {
            dequeFirstTimestampCandles()
          }
        case _ =>
      }

    }

    override def shape: FlowShape[Candlestick, immutable.Iterable[StreamElement]] = FlowShape(in, out)

  }

  /**
    * Listens to events from server and converts them to candlesticks
    *
    * @param factory              factory generating candlesticks from event
    * @param interval             interval of candlesticks
    * @param countdownStartMillis countdown for candlestick intevals
    */
  class CandleAggregatorFlow(factory: (DealInfo) => Candlestick, interval: FiniteDuration, countdownStartMillis: Long)
    extends GraphStage[FlowShape[TimestampedElement, Candlestick]] {

    val in: Inlet[TimestampedElement] = Inlet("InDeals")
    val out: Outlet[Candlestick] = Outlet("OutCandles")

    @scala.throws[Exception](classOf[Exception])
    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

      private var timeDiff: Long = 0

      private val candlesticks = mutable.Map.empty[String, Candlestick]
      private val outputQueue = mutable.Queue.empty[Candlestick]

      setHandler(in, new InHandler {
        @scala.throws[Exception](classOf[Exception])
        override def onPush() = {
          val elem = grab(in)
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
                    enqueueCandlesForPush
                    pushIfCanOtherwisePull
                  }
                case None =>
                  if (needTriggerPush(deal.timestamp)) {
                    enqueueCandlesForPush
                    candlesticks.update(deal.ticker, Candlestick.createOneMin(deal))
                    pushIfCanOtherwisePull
                  } else {
                    candlesticks.update(deal.ticker, Candlestick.createOneMin(deal))
                    pull(in)
                  }
              }
            case Tick =>
              val localTimeToServerTime = Instant.ofEpochMilli(elem.timestampMillis - timeDiff)
              if (needTriggerPush(localTimeToServerTime)) {
                enqueueCandlesForPush
                pushIfCanOtherwisePull
              } else {
                pull(in)
              }
          }
        }
      })

      setHandler(out, new OutHandler {
        @scala.throws[Exception](classOf[Exception])
        override def onPull() = {
          pushIfCanOtherwisePull
        }
      })

      private def pushIfCanOtherwisePull =
        if (outputQueue.nonEmpty) {
          push(out, outputQueue.dequeue())
        } else {
          if (!hasBeenPulled(in)) {
            pull(in)
          }
        }

      private def needTriggerPush(timestamp: Instant): Boolean = candlesticks.nonEmpty && dealInterval(timestamp) != candlesticks.head._2.timestamp

      private def enqueueCandlesForPush = {
        candlesticks foreach (e => outputQueue.enqueue(e._2))
        candlesticks.clear()
      }

      private def dealInterval(ts: Instant) = Candlestick.intervalStartForTimestamp(ts, interval, countdownStartMillis)
    }

    override def shape: FlowShape[TimestampedElement, Candlestick] = FlowShape(in, out)
  }

}

trait Candlestick extends StreamElement {

  import Candlestick._

  def ticker: String

  def openTimestamp: Instant

  def closeTimestamp: Instant

  def open: Double

  def high: Double

  def low: Double

  def close: Double

  def volume: Int

  def merge(data: DealInfo): Candlestick

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

/**
  * Чтобы отличать свечи с разным периодом объявляем отдельный тип для каждого
  */
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