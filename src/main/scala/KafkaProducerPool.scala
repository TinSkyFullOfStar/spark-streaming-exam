package main.scala


import java.util.Properties

import scala.collection.mutable

import org.apache.kafka.clients.producer.KafkaProducer
/**
  * Created by tinsky on 17-2-19.
  */
object KafkaProducerPool{
  private val props: Properties = new Properties()
  private val producerList = new mutable.ListMap[Int,KafkaProducer[String,String]]

  props.put("bootstrap.servers","localhost:9092")
  props.put("acks","all")
  props.put("retries", new Integer(0))
  props.put("batch.size", new Integer(16384))
  props.put("linger.ms", new Integer(1))
  props.put("buffer.memory", new Integer(33554432))
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  var producer:KafkaProducer[String,String] = null

  def setProperties(pro: Properties){
    for(i <- 1 to 10){
      producer = new KafkaProducer(pro)
      KafkaProducerPool.producerList.put(i,producer)
    }
  }

  def setProperty(key:String,value: Object){
    props.put(key,value)
    setProperties(props)
  }

  def getProducer() = {
    var result: KafkaProducer[String,String] = null

    if (producerList.isEmpty)
      for(i <- 1 to 10){
        producer = new KafkaProducer(props)
        KafkaProducerPool.producerList.put(i,producer)
      }
    else
      for(i <- 1 to 10 ){
        if (producerList.get(i)!=null){
          result = producerList.get(i).get
          producerList.put(i,null)
        }else if (i==10)
          result = null
      }

    result
  }

  def returnProducer(rProducer: KafkaProducer[String,String]){
    for (i <- 1 to 10)
      if (producerList.get(i)==null)
        producerList.put(i,rProducer)
  }

}
