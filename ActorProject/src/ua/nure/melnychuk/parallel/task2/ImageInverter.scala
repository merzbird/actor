package ua.nure.melnychuk.parallel.task2

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
import java.awt.image.BufferedImage
import java.io.File
import javax.imageio.ImageIO

object ImageInverter extends App {

  invertImage(nrOfWorkers = 4, image = "photo.jpg")

  sealed trait Message
  case object Invert extends Message
  case class Work(y: Int, h: Int, master: ActorRef) extends Message
  case class Result(bi: BufferedImage, duration: Duration)

  class Worker(bi: BufferedImage) extends Actor {
    val w = bi.getWidth
    def receive = {
      case Work(y, h, master) => {
        var rgbArray = bi.getRGB(0, y, w, h, null, 0, w).map(0xFFFFFF - _)
        bi.setRGB(0, y, w, h, rgbArray, 0, w)
        master ! true
      }
    }
  }

  class Master(nrOfWorkers: Int, bi: BufferedImage, listener: ActorRef) extends Actor {
    var nrOfResults: Int = _
    val start: Instant = Instant.now

    val workerRouter = {
      val routees = Vector.fill(nrOfWorkers) {
        val r = context.actorOf(Props(new Worker(bi)))
        context watch r
        ActorRefRoutee(r)
      }
      Router(RoundRobinRoutingLogic(), routees)
    }

    def receive = {
      case Invert =>
        var y = 0
        for (h <- segments(bi.getHeight, nrOfWorkers)) {
          workerRouter.route(Work(y, h, self), sender)
          y += h
        }
      case true =>
        nrOfResults += 1
        if (nrOfResults == nrOfWorkers) {
          listener ! Result(bi, duration = Duration.between(start, Instant.now))
          context.stop(self)
        }
    }

    def segments(height: Int, numberOfSegments: Int) = {
      val segments = new Array[Int](numberOfSegments)
      for (i <- 0 until numberOfSegments) {
        var h = 0
        if (i < numberOfSegments - 1) {
          segments(i) = height / numberOfSegments
        } else {
          segments(i) = height - (height / numberOfSegments) * (numberOfSegments - 1)
        }
      }
      segments
    }
  }

  class Listener extends Actor {
    def receive = {
      case Result(bi, duration) =>
        println("Multithread Time: \t%s".format(duration.toMillis()))
        ImageIO.write(bi, "jpg", new File("multithread.jpg"))
        context.system.terminate()
    }
  }

  def invertImageSingleThread(nrOfWorkers: Int, image: String) {
    var bi = ImageIO.read(new File(image))
    var start = Instant.now
    var w = bi.getWidth
    var h = bi.getHeight
    var rgbArray = bi.getRGB(0, 0, w, h, null, 0, w).map(0xFFFFFF - _)
    bi.setRGB(0, 0, w, h, rgbArray, 0, w)
    ImageIO.write(bi, "jpg", new File("singlethread.jpg"))
    println("Singlethread Time: \t%s".format(Duration.between(start, Instant.now).toMillis()))
  }

  def invertImageMultithread(nrOfWorkers: Int, image: String) {
    val system = ActorSystem("ImageInverter")
    val listener = system.actorOf(Props[Listener], name = "listener")
    val master = system.actorOf(Props(new Master(nrOfWorkers, ImageIO.read(new File(image)), listener)), name = "master")
    master ! Invert
  }

  def invertImage(nrOfWorkers: Int, image: String) {
    invertImageSingleThread(nrOfWorkers, image)
    invertImageMultithread(nrOfWorkers, image)
  }

}