package ua.nure.melnychuk.parallel.task1

import java.time.Duration
import java.time.Instant

import scala.util.Random

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.routing.ActorRefRoutee
import akka.routing.RoundRobinRoutingLogic
import akka.routing.Router

object Average extends App {

  calculate(nrOfWorkers = 8, nrOfElements = 1000000)

  sealed trait Message
  case object Calculate extends Message
  case class Work(array: Array[Int], master: ActorRef) extends Message
  case class Result(value: Double) extends Message
  case class AverageCalculation(avg: Double, duration: Duration)

  class Worker extends Actor {
    def receive = {
      case Work(array, master) => master ! Result(calculateAvg(array))
    }
    def calculateAvg(array: Array[Int]): Double = {
      (array.sum.toDouble / array.length).toDouble
    }
  }

  class Master(nrOfWorkers: Int, array: Array[Int], listener: ActorRef) extends Actor {
    var sumOfAvg: Double = _
    var nrOfResults: Int = _
    val start: Instant = Instant.now

    val workerRouter = {
      val routees = Vector.fill(nrOfWorkers) {
        val r = context.actorOf(Props[Worker])
        context watch r
        ActorRefRoutee(r)
      }
      Router(RoundRobinRoutingLogic(), routees)
    }

    def receive = {
      case Calculate =>
        for ((bottom, top) <- segment(array.length, nrOfWorkers)) {
          workerRouter.route(Work(array.slice(bottom, top), self), sender)
        }
      case Result(value) =>
        sumOfAvg += value
        nrOfResults += 1
        if (nrOfResults == nrOfWorkers) {
          listener ! AverageCalculation(avg = sumOfAvg / nrOfResults.toDouble, duration = Duration.between(start, Instant.now))
          context.stop(self)
        }
    }

    def segment(problemSize: Int, numberOfSegments: Int) = {
      val segmentSize = ((problemSize + 1) toDouble) / numberOfSegments
      def intFloor(d: Double) = (d floor) toInt;
      for {
        i <- 0 until numberOfSegments
        bottom = intFloor(i * segmentSize)
        top = intFloor((i + 1) * segmentSize) min (problemSize)
      } yield ((bottom, top))
    }

  }

  class Listener extends Actor {
    def receive = {
      case AverageCalculation(avg, duration) =>
        println("MultiThread\nAvg: \t\t\t%f\nCalculation time: \t%s".format(avg, duration.toMillis()))
        context.system.terminate()
    }
  }

  def calculate(nrOfWorkers: Int, nrOfElements: Int) {
    val array = new Array[Int](nrOfElements)
    for (i <- 0 until nrOfElements) { array(i) = 1 + Random.nextInt(10) }
    calculateSingleThread(array)
    calculateMultiThread(nrOfWorkers, array)
  }

  def calculateSingleThread(array: Array[Int]) {
    var start = Instant.now()
    var avg = (array.sum.toDouble / array.length).toDouble
    println("SingleThread\nAvg: \t\t\t%f\nCalculation time: \t%s".format(avg, Duration.between(start, Instant.now).toMillis()))
  }

  def calculateMultiThread(nrOfWorkers: Int, array: Array[Int]) {
    val system = ActorSystem("AverageCounterSystem")
    val listener = system.actorOf(Props[Listener], name = "listener")
    val master = system.actorOf(Props(new Master(nrOfWorkers, array, listener)), name = "master")
    master ! Calculate
  }
}