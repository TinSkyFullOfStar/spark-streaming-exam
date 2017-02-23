package test


/**
  * Created by tinsky on 17-2-19.
  */
object Ma {
  def main(args: Array[String]): Unit = {
   val sender = new KafkaSender
    sender.pushDataToKafka()
  }
}
