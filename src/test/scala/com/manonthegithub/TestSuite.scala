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
}
