package main.scala

import akka.actor.UntypedAbstractActor
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.json.JSONObject
import org.json.JSONArray

import scala.collection.mutable.ArrayBuffer
import scalaj.http.{Http, HttpResponse}
/**
  * Created by tinsky on 17-2-20.
  */
class DataSourceActor extends UntypedAbstractActor{
  private var page = 1
  private val baseUrl = "http://web.juhe.cn:8080/finance/stock/"
  private val secretKey = "1ddda6ea3fd8bfcfc3398ccd1f7cf0ce"

  private val urlArr = Array(
                        baseUrl+"hkall?key="+secretKey+"&type=4"+"&page=",
//                        baseUrl+"usaall?key="+secretKey+"&type=4"+"&page=",
                        baseUrl+"szall?key="+secretKey+"&type=4"+"&page=",
                        baseUrl+"shall?key="+secretKey+"&type=4"+"&page=")

  override def onReceive(message: Any):Unit = {
    var jsonArray:JSONArray = null
    var producer:KafkaProducer[String,String] = null
    val recordsArr = new Array[Array[ProducerRecord[String,String]]](4)

//    while(producer==null){
//      producer = KafkaProducerPool.getProducer()
//    }

    for(i <- urlArr.indices){
      jsonArray = getData(i)
//      result = formatData(jsonArray)
      page = 1
    }



//    for(i <- recordsArr.indices;j <- recordsArr(i).indices)
//      println("hello world")
    //      producer.send(recordsArr(i)(j))
    KafkaProducerPool.returnProducer(producer)
  }

  def getData(i:Int):JSONArray ={
    var url = ""
    var json: JSONObject = new JSONObject("{\"error_code\":0}")
    var response: HttpResponse[String] = null
    val jsonArr:JSONArray = new JSONArray()

    while(json.getString("error_code").equals("0")){
      if(page != 1)
        jsonArr.put(json)
      url = urlArr(i) + page
      response = Http(url).asString
      println(response.body)
      json = new JSONObject(response.body)
      jsonArr.put(json)
      page += 1
    }

    jsonArr
  }

  def formatData(jsonArr:JSONArray):Array[ProducerRecord[String,String]] ={
    var jsonArray:JSONArray = null
    val arr: ArrayBuffer[ProducerRecord[String,String]] = new ArrayBuffer[ProducerRecord[String, String]]()

    for(i <- 0 until jsonArr.length()){
      if(page != 1)
        jsonArray = jsonArr.getJSONObject(i).getJSONObject("result").getJSONArray("data")

      for(i <- 0 until jsonArray.length()){
        val keys = jsonArray.getJSONObject(i).keys()

        while(keys.hasNext){
            arr.append(new ProducerRecord[String,String]("world",
              keys.next().toString,
              jsonArray.getJSONObject(i).getString(keys.next().toString))
          )
        }

      }
    }

    arr.toArray
  }

}
