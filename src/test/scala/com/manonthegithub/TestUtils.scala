package com.manonthegithub

import java.time.Instant

import scala.util.Random

/**
  * Created by Kirill on 15/02/2017.
  */
object TestUtils {

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

}
