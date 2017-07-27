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
     For simplicity, there is an assumption that highest bit is always 0,
     even though all primitive numeric types in Java/Scala are signed.
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

  //Using MergeHub and BroadcastHub to isolate server connections from client ones,
  //i.e. server connection failure won't affect client connections and vice versa.
  import JsonFormats.CandleStickJsonFormat
  import spray.json._
  import scala.concurrent.duration._

  //parse raw byte messages and combine them to candlesticks
  val RawDataToCandlesBroadcastFLow = DealInfo
    .framingConverterFlow
    //we need to sync local time with server time and keep alive even if server stopped writing
    .keepAlive(3 seconds, () => Tick)
    .map(TimestampedElement(_, System.currentTimeMillis()))
    .via(Candlestick.flowOfOneMin)

  //combine consumption of packets from server with broadcasting to clients
  val ConnectedServerClientGraph = MergeHub
    .source[ByteString](perProducerBufferSize = 1)
    .via(RawDataToCandlesBroadcastFLow)
    //first broadcast candlesticks from buffer
    .alsoToMat(Candlestick
      .tenMinBufferOfOneMin
      .toMat(BroadcastHub.sink(1))(Keep.right))(Keep.both)
    //then broadcast candlesticks in real time
    .toMat(BroadcastHub.sink(1))(Keep.both)

  val ((mergeSink, broadcastBufferSource), broadcastSource) = ConnectedServerClientGraph.run()

  // Connections with server
  Source
    .repeat(Tcp().outgoingConnection(Host, PortToConnect))
    //we have only one simultaneous connection with server
    .mapAsync(1)(
      Source
        .single(ByteString.empty)
        .via(_)
        .watchTermination()(Keep.right)
        // Isolate server connections with MergeHub
        .toMat(mergeSink)(Keep.left)
        .run()
        .recover{ case _ => Done }
    )
    //retry connection every 5 seconds on failure
    .delay(5 seconds, DelayOverflowStrategy.dropNew)
    .runWith(Sink.ignore)

  val BufferRawSource = broadcastBufferSource
    .log("TenBatch")
    .drop(1)
    .mapConcat[StreamElement](b => b)
    .takeWhile(_.isInstanceOf[Candlestick])
    .map(_.asInstanceOf[Candlestick])

  // Connections with clients
  Tcp()
    .bind(Host, PortToBind)
    .map(_.flow)
    // Isolate client connections with BroadcastHub
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

//model of stock events from server
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

  //raw bytes to DealInfo objects
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



