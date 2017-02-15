name := "ex-deals-stream"

version := "1.0"

scalaVersion := "2.12.1"

val akkaVersion = "2.4.17"

libraryDependencies ++= Seq(
  "io.spray" %%  "spray-json" % "1.3.3",
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  //test scope
  "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % "test",
  "org.scalatest" %% "scalatest" % "3.0.0" % "test"
)

mainClass in assembly := Some("com.manonthegithub.StreamConsumer")