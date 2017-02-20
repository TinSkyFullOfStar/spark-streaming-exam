package main.scala


import java.util.concurrent.{Delayed, TimeUnit}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.json.{JSONArray, JSONObject}

import scalaj.http.{Http, HttpResponse}
/**
  * Created by tinsky on 17-2-17.
  */
class HttpTask(time:Long) extends TaskTrait{
  val delayedTime: Long = TimeUnit.NANOSECONDS.convert(time,TimeUnit.SECONDS)+System.nanoTime()//将time分钟转换成微秒为单位

  //对比排序
  def compareTo(t:Delayed): Int ={
    if (t==null||t.isInstanceOf[HttpTask])
      1
    else if (this == t)
      0
    else{
      val task = t.asInstanceOf[HttpTask]

      var result:Int = 0
      if (this.delayedTime > task.delayedTime)
        result = 1
      else if (this.delayedTime == task.delayedTime)
        result = 0
      else if (this.delayedTime < task.delayedTime)
        result = -1

      result
    }
  }

  //跑任务
  override def run() = {
    var page = 1
    var url = ""
    var json: JSONObject = new JSONObject("{\"error_code\":0}")
    var response: HttpResponse[String] = null
    var producer:KafkaProducer[String,String] = null
    val secretKey = "your-juhe-api-appKey"

    while(producer==null){
      producer = KafkaProducerPool.getProducer()
    }

    while(json.getString("error_code").equals("0")||page == 1){
     if(page != 1){
       val jsonArray = json.getJSONObject("result").getJSONArray("data")

       for(i <- 0 until jsonArray.length()){
         val keys = jsonArray.getJSONObject(i).keys()
         while(keys.hasNext){
           producer.send(new ProducerRecord[String,String]("world",
             keys.next().toString,
             jsonArray.getJSONObject(i).getString(keys.next().toString))
           )
         }
       }

     }

      url = "http://web.juhe.cn:8080/finance/stock/hkall?key="+secretKey+"&page="+page+"&type=4"
      response = Http(url).asString
      json = new JSONObject(response.body)
      page += 1
    }

    KafkaProducerPool.returnProducer(producer)
  }

  //返回值确定任务是否到达或者超过延迟时间，0或者负数表示到达或者超过
  override def getDelay(unit: TimeUnit) = unit.convert(delayedTime-System.nanoTime(),TimeUnit.NANOSECONDS)
}
