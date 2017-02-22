package test

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem, Props}
import main.scala.DataSourceActor

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by tinsky on 17-2-19.
  */
object Ma {
  def main(args: Array[String]): Unit = {
    val actorSystem = ActorSystem.create("test")
    val actRef: ActorRef = actorSystem.actorOf(Props[DataSourceActor],"first")

    val message = 1
    val callBack = actorSystem.scheduler.schedule(FiniteDuration(0,TimeUnit.NANOSECONDS),
                  FiniteDuration(500,TimeUnit.MILLISECONDS),
                  actRef,
                  message)

    Thread.sleep(7000)

    callBack.cancel()
    actorSystem.terminate()
  }
}
