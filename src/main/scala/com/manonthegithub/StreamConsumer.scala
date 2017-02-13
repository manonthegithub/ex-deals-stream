package com.manonthegithub

import java.nio.ByteOrder
import java.nio.charset.StandardCharsets
import java.time._
import java.time.temporal.ChronoUnit

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString

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

  import scala.concurrent.duration._

  val intervalTicker = Source
    .tick(0 seconds, 100 millis, () => System.currentTimeMillis())
    .log("LocalTime")

  val MessageToCandleFlow = {
    import JsonFormats.CandleStickJsonFormat
    import scala.collection.immutable._

    def now = System.currentTimeMillis()

    DealInfo
      .framingConverterFlow
      //нужно, чтобы синхронизировать время на сервере со временем элементов
      //и гарантированно выдавать свечи, даже если сервер перестал писать данные
      .keepAlive(3 seconds, () => Tick)
      .zipWith(Source.repeat[() => Long](System.currentTimeMillis).map(_ ()))(TimestampedElement(_, _))
      .via(Candlestick.flowOfOneMin)
      .map(_ => ByteString("123"))
    //      .groupBy(60, m => LocalDateTime.ofInstant(m.timestamp, ZoneId.systemDefault()).getMinute)
    //      .groupBy(NumberOfSupportedTickers, _.ticker)
    //        .map(Candlestick.createOneMin)
    //        .reduce(_.merge(_))
    //      .mergeSubstreams
    //        .fold[ByteString](ByteString.empty)((b, c) => b ++ ByteString(c.asInstanceOf[Candlestick].toJson.compactPrint))
    //        .log("Candle")
    //      .concatSubstreams


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

object DealInfo {

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
      fieldLength = DealInfo.FrameFieldLengthInBytes,
      fieldOffset = 0,
      maximumFrameLength = DealInfo.MaxFrameLength,
      byteOrder = ByteOrder.BIG_ENDIAN
    )
    .map(DealInfo.fromRawBytes)
    .log("Parsed messages")

  def fromRawBytes(bs: ByteString): DealInfo = {
    val bb = bs.toByteBuffer
    val tickerLen = bb.getShort(TickerFieldLenOffset)
    val time = Instant.ofEpochMilli(bb.getLong(FrameFieldLengthInBytes))
    val price = bb.getDouble(priceOffset(tickerLen))
    val size = bb.getInt(sizeOffset(tickerLen))
    val ticker = bs.drop(TickerOffset).take(tickerLen).decodeString(StandardCharsets.US_ASCII)

    DealInfo(time, ticker, price, size)
  }

  private def priceOffset(tickerLen: Int) = TickerOffset + tickerLen

  private def sizeOffset(tickerLen: Int) = priceOffset(tickerLen) + PriceFieldLen

}

case class DealInfo(timestamp: Instant, ticker: String, price: Double, size: Int) extends StreamElement
case object Tick extends StreamElement
trait StreamElement

case class TimestampedElement(element: StreamElement, timestampMillis: Long)


