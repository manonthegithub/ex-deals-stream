package com.manonthegithub

import spray.json._

/**
  * Created by Kirill on 13/02/2017.
  */
object JsonFormats extends DefaultJsonProtocol {

  //{ "ticker": "AAPL", "timestamp": "2016-01-01T15:02:00Z", "open": 101.1, "high": 101.3, "low": 101, "close": 101, "volume": 1300 }
  implicit object CandleStickJsonFormat extends RootJsonFormat[Candlestick]{
    override def write(obj: Candlestick): JsValue = JsObject(
      "ticker" -> JsString(obj.ticker),
      "timestamp" -> JsString(obj.timestamp.toString),
      "open" -> JsNumber(obj.open),
      "high" -> JsNumber(obj.high),
      "low" -> JsNumber(obj.low),
      "close" -> JsNumber(obj.close),
      "volume" -> JsNumber(obj.volume)
    )

    override def read(json: JsValue): Candlestick = ???
  }

}
