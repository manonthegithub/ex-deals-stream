package com.manonthegithub

import java.nio.ByteOrder
import java.nio.charset.StandardCharsets
import java.time._
import java.time.temporal.ChronoUnit

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString
import spray.json._

import scala.concurrent.duration.FiniteDuration




/**
  * Created by Kirill on 10/02/2017.
  */
object StreamConsumer extends App {

  /*
     Несмотря на то, что в Java/Scala все примитивные числовые типы - знаковые,
     в рамках тестового задания не будем делать лишние конвертации,
     считаем, что везде при конвертации в приходящих значениях
     старший байт будет всегда 0
  */

  implicit val system = ActorSystem("stock-exchange")

  val decider: Supervision.Decider = {
    case _ => Supervision.Resume
  }
  val MatSettings = ActorMaterializerSettings(system)
    .withSupervisionStrategy(decider)
  implicit val mat = ActorMaterializer(MatSettings)
  implicit val ec = system.dispatcher

  val Host = "localhost"
  val PortToConnect = 15555
  val PortToBind = 15556

  val NumberOfSupportedTickers = 4


  import scala.concurrent.duration._
  val intervalTicker = Source
    .tick(durationToNearestMinute, 1 minute, LocalDateTime.now())
    .log("LocalTime")

  val MessageToCandleFlow = {
    import JsonFormats.CandleStickJsonFormat

    IncomingMessage
      .framingConverterFlow
      .groupBy(60, m => LocalDateTime.ofInstant(m.timestamp, ZoneId.systemDefault()).getMinute)
      .groupBy(NumberOfSupportedTickers, _.ticker)
        .map(Candlestick.createOneMin)
        .reduce(_.merge(_))
      .mergeSubstreams
        .fold[ByteString](ByteString.empty)((b, c) => b ++ ByteString(c.asInstanceOf[Candlestick].toJson.compactPrint))
        .log("Candle")
      .concatSubstreams


  }

  //Используем MergeHub и BroadcastHub для того,
  //чтобы отвязать серверные коннекты от клиентских,
  //т.е. если пропадает связь с сервером, то клиенты не отваливаются,
  //а ждут пока она восстановится,
  //в свою очередь клиенты могут отвалиться и добавляться,
  //что не влияет на серверный коннект




  val BroadcastConsumer = MessageToCandleFlow
    .toMat(BroadcastHub.sink(256))(Keep.right)

  val ConnectedServerClientGraph = MergeHub
    .source[ByteString](perProducerBufferSize = 16)
    .toMat(BroadcastConsumer)(Keep.both)

  val (mergeSink, broadcastSource) = ConnectedServerClientGraph.run()

  // Работа серверными соединениями
  Source
    .repeat(Tcp().outgoingConnection(Host, PortToConnect))
    // одно одновременное подключение
    .mapAsync(1)(
    Source
      .single(ByteString.empty)
      .via(_)
      .watchTermination()(Keep.right)
      // использовать мёрдж хаб для коннектов к серверу с данными
      .toMat(mergeSink)(Keep.left)
      .run()
  ).runWith(Sink.ignore)


  // Работа с клиентскими соединениями
  Tcp()
    .bind(Host, PortToBind)
    .map(_.flow)
    // используем бродкаст для передачи сообщений на клиенты
    .map(broadcastSource
      .log("Sent to client")
      .via(_)
      .runWith(Sink.ignore)
    ).runWith(Sink.ignore)




  def durationToNearestMinute: FiniteDuration = {
    val now = LocalDateTime.now()
    val nextMinuteStart = LocalDateTime.of(
      now.getYear,
      now.getMonth,
      now.getDayOfMonth,
      now.getHour,
      now.getMinute + 1,
      0,
      0)

    (now.until(nextMinuteStart, ChronoUnit.SECONDS) + 1) seconds
  }

}

object Candlestick {

  def createOneMin(data: IncomingMessage): CandlestickOneMinute =
    CandlestickOneMinute(
      data.ticker,
      data.timestamp,
      data.timestamp,
      data.price,
      data.price,
      data.price,
      data.price,
      data.size)

}

trait Candlestick {

  def ticker: String
  def openTimestamp: Instant
  def closeTimestamp: Instant
  def open: Double
  def high: Double
  def low: Double
  def close: Double
  def volume: Int

  //время начала интервала свечи
  final def timestamp: Instant = {
    val millisMultiplier = (openTimestamp.toEpochMilli - startingPointForCountdownMillis) / interval.toMillis
    val millis = startingPointForCountdownMillis + (millisMultiplier * interval.toMillis)
    Instant.ofEpochMilli(millis)
  }

  def interval: FiniteDuration

  def startingPointForCountdownMillis: Long = 0
}

case class CandlestickOneMinute(ticker: String,
                                openTimestamp: Instant,
                                closeTimestamp: Instant,
                                open: Double,
                                high: Double,
                                low: Double,
                                close: Double,
                                volume: Int) extends Candlestick{
  import scala.concurrent.duration._
  import scala.math._

  def merge(data: IncomingMessage): CandlestickOneMinute =
    if (this.ticker == data.ticker) {
      val distance = data.timestamp.toEpochMilli - this.timestamp.toEpochMilli
      if (distance >= 0 && distance.millis < interval) {
        val (newOpenTimestamp, newCloseTimestamp, newOpen, newClose) = if(data.timestamp.compareTo(openTimestamp) >= 0) {
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

  def merge(other: CandlestickOneMinute) =
    if (this.ticker == other.ticker) {
      if (other.timestamp.toEpochMilli == this.timestamp.toEpochMilli) {
        val (newOpenTimestamp, newOpen) =
          if(other.openTimestamp.compareTo(openTimestamp) >= 0) (openTimestamp, open)
          else (other.openTimestamp, other.open)
        val (newCloseTimestamp, newClose) =
          if(other.closeTimestamp.compareTo(closeTimestamp) <= 0) (closeTimestamp, close)
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


  def interval: FiniteDuration = 1 minute
}

object IncomingMessage {

  val FrameFieldLengthInBytes = 2
  val MaxFrameLength = Int.MaxValue >>> (32 - 8 * FrameFieldLengthInBytes)

  val TimestampFieldLen = 8
  val TickerLenFieldLen = 2
  val PriceFieldLen = 8
  val SizeFieldLen = 4

  private val TickerFieldLenOffset = FrameFieldLengthInBytes + TimestampFieldLen
  private val TickerOffset = FrameFieldLengthInBytes + TimestampFieldLen + TickerLenFieldLen


  def framingConverterFlow = Framing
    .lengthField(
      fieldLength = IncomingMessage.FrameFieldLengthInBytes,
      fieldOffset = 0,
      maximumFrameLength = IncomingMessage.MaxFrameLength,
      byteOrder = ByteOrder.BIG_ENDIAN
    )
    .map(IncomingMessage.fromRawBytes)
    .log("Parsed messages")

  def fromRawBytes(bs: ByteString): IncomingMessage = {
    val bb = bs.toByteBuffer
    val tickerLen = bb.getShort(TickerFieldLenOffset)
    val time = Instant.ofEpochMilli(bb.getLong(FrameFieldLengthInBytes))
    val price = bb.getDouble(priceOffset(tickerLen))
    val size = bb.getInt(sizeOffset(tickerLen))
    val ticker = bs.drop(TickerOffset).take(tickerLen).decodeString(StandardCharsets.US_ASCII)

    IncomingMessage(time, ticker, price, size)
  }

  private def priceOffset(tickerLen: Int) = TickerOffset + tickerLen

  private def sizeOffset(tickerLen: Int) = priceOffset(tickerLen) + PriceFieldLen

}

case class IncomingMessage(timestamp: Instant, ticker: String, price: Double, size: Int)


