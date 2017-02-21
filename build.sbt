organization := "LittleStar"

name := "spark-streaming-exam"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
		"org.apache.spark" %% "spark-core_2.11" % "2.1.0"
		"org.apache.spark" %% "spark-streaming_2.11" % "2.1.0"
		"org.apache.spark" %% "spark-streaming-kafka-0-10_2.11" % "2.1.0"
		"org.apache.kafka" %% "kafka-clients" % "0.10.0.0"
		"com.typesafe.akka" % "akka-actor_2.11" % "2.5-M1"
		"com.typesafe" % "config" % "1.3.0"
	)
