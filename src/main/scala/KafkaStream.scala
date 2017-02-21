package main.scala

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

/**
  * Created by tinsky on 17-2-2.
  */
object KafkaStream {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("H").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(2))

    ssc.checkpoint("/tmp/")

    val broker = "localhost:9092"
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> broker,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" ->"use_a_separate_group_id_for_each_stream",
      "auto.offset.rest" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("world")

    val stream = KafkaUtils.createDirectStream[String,String](
      ssc,
      PreferConsistent,
      Subscribe[String,String](topics,kafkaParams))
    val pairs = stream.map(record => (record.value,1))
    val count = pairs.updateStateByKey(updateFunc)

    count.print()
    ssc.start()
    ssc.awaitTermination()

  }


  def updateFunc(newValue: Seq[Int],previousValue: Option[Int]) ={
    val newVal = newValue.sum + previousValue.getOrElse(0)
    Some(newVal)
  }

}
