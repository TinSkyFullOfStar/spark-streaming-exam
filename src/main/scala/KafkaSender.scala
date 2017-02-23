package main.scala

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem, Props}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.ExecutionContext.Implicits.global


/**
  * Created by tinsky on 17-2-17.
  */
class KafkaSender{
  private var runtime: Long = 15000
  private var timeSpan: Long = 500
  private var delayedTime: Long = 0
  private var delayedTimeUnit: TimeUnit = TimeUnit.NANOSECONDS
  private var timeSpanUnit: TimeUnit = TimeUnit.MILLISECONDS

  def setTerminalTime(time: Long): Unit ={
      this.runtime = time
  }

  def setTimeSpan(span:Long): Unit ={
    this.timeSpan = span
  }

  def setDelayedTime(delayed: Long): Unit ={
    this.delayedTime = delayed
  }

  def setDelayedTimeUnit(delayedUnit: TimeUnit): Unit ={
    this.delayedTimeUnit = delayedUnit
  }

  def setTimeSpanUnit(spanUnit: TimeUnit): Unit ={
    this.timeSpanUnit =spanUnit
  }

  def pushDataToKafka(): Unit ={
    val actorSystem = ActorSystem.create("test")
    val actRef: ActorRef = actorSystem.actorOf(Props[DataSourceActor],"first")

    val message = 1
    val callBack = actorSystem.scheduler.schedule(FiniteDuration(delayedTime,delayedTimeUnit),
      FiniteDuration(timeSpan,TimeUnit.MILLISECONDS),
      actRef,
      message)

    Thread.sleep(runtime)

    callBack.cancel()
    actorSystem.terminate()
  }

}