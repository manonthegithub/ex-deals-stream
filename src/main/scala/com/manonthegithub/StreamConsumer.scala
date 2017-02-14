package com.manonthegithub

import java.nio.ByteOrder
import java.nio.charset.StandardCharsets
import java.time._

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString

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

  val TenMinBufferBroadcastSink = Flow[Candlestick]
    .statefulMapConcat[StreamElement](batchCandles)
    .toMat(BroadcastHub.sink(256))(Keep.right)

  import JsonFormats.CandleStickJsonFormat
  import spray.json._

  //конвертируем данные из битовых строк в данные по сделке и аггрегируем в свечи
  val MessageToCandleFlow = {
    import scala.concurrent.duration._

    DealInfo
      .framingConverterFlow
      //нужно, чтобы синхронизировать время на сервере со временем элементов
      //и гарантированно выдавать последню свечь перед паузой, даже если сервер перестал писать данные
      .keepAlive(1 seconds, () => Tick)
      .zipWith(Source
        .repeat[() => Long](System.currentTimeMillis)
        .map(_ ()))(TimestampedElement(_, _))
      .via(Candlestick.flowOfOneMin)
      .alsoToMat(TenMinBufferBroadcastSink)(Keep.right)
      .map(c => ByteString(c.toJson.compactPrint))
      .intersperse(ByteString("\n"))
  }

  //Используем MergeHub и BroadcastHub для того,
  //чтобы отвязать серверные коннекты от клиентских,
  //т.е. если пропадает связь с сервером, то клиенты не отваливаются,
  //а ждут пока она восстановится,
  //в свою очередь клиенты могут отвалиться и добавляться,
  //что не влияет на серверный коннект

  val BroadcastConsumer = MessageToCandleFlow
    .toMat(BroadcastHub.sink(256))(Keep.both)

  val ConnectedServerClientGraph = MergeHub
    .source[ByteString](perProducerBufferSize = 16)
    .toMat(BroadcastConsumer)(Keep.both)

  val (mergeSink,(broadcastBufferSource, broadcastSource)) = ConnectedServerClientGraph.run()

  val BufferRawSource = broadcastBufferSource
    .takeWhile(_.isInstanceOf[Candlestick])
    .map(_.asInstanceOf[Candlestick])
    .log("TenBatch")
    .map(c => ByteString(c.toJson.compactPrint))
    .intersperse(ByteString("\n"))

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
    .map(broadcastSource.prepend(BufferRawSource)
    .log("Sent to client")
    .via(_)
    .runWith(Sink.ignore)
  ).runWith(Sink.ignore)

  import scala.collection.immutable._
  def batchCandles(): Function[Candlestick, Iterable[StreamElement]] = {
    import scala.collection.immutable.Queue
    val MaxBufferIntervals = 10
    var buffer = Queue.empty[Candlestick]
    var count = 0

    def aggregate(c: Candlestick) = {
      if (buffer.isEmpty || buffer.last.timestamp != c.timestamp) {
        count += 1
      }
      buffer :+= c
      val buf = if (count < MaxBufferIntervals) {
        buffer
      } else {
        var head: Candlestick = ???
        do {
          head = buffer.head
          buffer = buffer.tail
        } while (buffer.head.timestamp == head.timestamp)
        count -= 1

        buffer
      }
      buf :+ EndOfBatch
    }

    aggregate
  }

}

case class DealInfo(timestamp: Instant, ticker: String, price: Double, size: Int) extends StreamElement

case object Tick extends StreamElement

case object EndOfBatch extends StreamElement

trait StreamElement

case class TimestampedElement(element: StreamElement, timestampMillis: Long)

object DealInfo {

  val FrameFieldLengthInBytes = 2
  val MaxFrameLength = Int.MaxValue >>> (32 - 8 * FrameFieldLengthInBytes)

  val TimestampFieldLen = 8
  val TickerLenFieldLen = 2
  val PriceFieldLen = 8
  val SizeFieldLen = 4

  private val TickerFieldLenOffset = FrameFieldLengthInBytes + TimestampFieldLen
  private val TickerOffset = FrameFieldLengthInBytes + TimestampFieldLen + TickerLenFieldLen

  //бьём на фреймы, парсим каждый  фрейм
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



