package test


import main.scala._
/**
  * Created by tinsky on 17-2-19.
  */
object Test {
  def main(args: Array[String]): Unit = {
    val sender = new KafkaSender()
    new Thread(sender).start()
  }
}
