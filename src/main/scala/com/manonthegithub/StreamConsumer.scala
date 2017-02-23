package com.manonthegithub

import java.nio.ByteOrder
import java.nio.charset.StandardCharsets
import java.time._

import akka.Done
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString
import com.typesafe.config.ConfigFactory

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

  val conf = ConfigFactory
    .load()
    .getConfig("eds")

  val Host = conf.getString("remote.host")
  val PortToConnect = conf.getInt("remote.port")
  val PortToBind = conf.getInt("bind.port")

  //Используем MergeHub и BroadcastHub для того,
  //чтобы отвязать серверные коннекты от клиентских,
  //т.е. если пропадает связь с сервером, то клиенты не отваливаются,
  //а ждут пока она восстановится,
  //в свою очередь клиенты могут отвалиться и добавляться,
  //что не влияет на серверный коннект

  import JsonFormats.CandleStickJsonFormat
  import spray.json._
  import scala.concurrent.duration._

  //конвертируем данных из битовых строк в данные по сделке, далее, аггрегируем в свечи
  val rawDataToCandlesBroadcastSink = DealInfo
    //парсинг сырых данных
    .framingConverterFlow
    //нужно, чтобы синхронизировать время на сервере, передающем данные, c локальным временем,
    //и гарантированно выдавать последню свечь перед паузой, даже если сервер перестал писать данные
    .keepAlive(3 seconds, () => Tick)
    .map(TimestampedElement(_, System.currentTimeMillis()))
    //аггрегируем в свечи
    .via(Candlestick.flowOfOneMin)

  //теперь приём пакетов из серверных соединений
  //соединяем с бродкастингом на клиенты
  val ConnectedServerClientGraph = MergeHub
    .source[ByteString](perProducerBufferSize = 1)
    //пропускаем данные с сервера через конвертер в свечи
    .via(rawDataToCandlesBroadcastSink)
    //бродкастим свечи за последние 10 минут
    .alsoToMat(Candlestick
      .tenMinBufferOfOneMin
      .toMat(BroadcastHub.sink(1))(Keep.right))(Keep.both)
    //бродкастим свечи по одной
    .toMat(BroadcastHub.sink(1))(Keep.both)

  val ((mergeSink, broadcastBufferSource), broadcastSource) = ConnectedServerClientGraph.run()

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
        .recover{ case _ => Done }
    )
    //при разрыве соединения пытаемся восстановить его через 5 секунд
    .delay(5 seconds, DelayOverflowStrategy.dropNew)
    .runWith(Sink.ignore)

  //источник пачек свечей за последние 10 минут
  val BufferRawSource = broadcastBufferSource
    .log("TenBatch")
    .drop(1)
    .mapConcat[StreamElement](b => b)
    .takeWhile(_.isInstanceOf[Candlestick])
    .map(_.asInstanceOf[Candlestick])

  // Работа с клиентскими соединениями
  Tcp()
    .bind(Host, PortToBind)
    .map(_.flow)
    // используем бродкаст для передачи свечей на клиенты,
    // сначала за последние 10 минут потом остальные
    .map(broadcastSource
      .prepend(BufferRawSource)
      .map(c => ByteString(c.toJson.compactPrint))
      .intersperse(ByteString("\n"))
      .via(_)
      .runWith(Sink.ignore)
    ).runWith(Sink.ignore)

}

case class TimestampedElement(element: StreamElement, timestampMillis: Long)

case object Tick extends StreamElement

case object EndOfBatch extends StreamElement

trait StreamElement

//объектное представление сообщений с сервера
case class DealInfo(timestamp: Instant, ticker: String, price: Double, size: Int) extends StreamElement

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



