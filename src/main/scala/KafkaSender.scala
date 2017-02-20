package main.scala


import java.util.concurrent.DelayQueue

/**
  * Created by tinsky on 17-2-17.
  */
class KafkaSender(task: TaskTrait) extends Runnable{
  private val delayQueue:DelayQueue[TaskTrait] = new DelayQueue[TaskTrait]()
  private val menTask = task

  def this()={
    this(new HttpTask(9))
    delayQueue.add(this.menTask)
  }

  override def run() = {
    try{
        delayQueue.take().run()
    }catch {
      case e: Exception => e.printStackTrace()
    }
  }

  def addTask(t: TaskTrait) = {
    delayQueue.add(t)
    println("add successful")
  }
}
