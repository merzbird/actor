package ua.nure.melnychuk.parallel.task6

import scala.actors.Actor

object RecieveReactCompare extends App {

  class ReceiveActor extends Actor {
    def act {
      while (true) {
        receive {
          case "Hello" => println("ReceiveActor with thread %s".format(Thread.currentThread))
        }
      }
    }
  }

  class ReactActor extends Actor {
    def act {
      loop {
        react {
          case "Hello" => println("ReactActor with thread %s".format(Thread.currentThread))
        }
      }
    }
  }

  (for (i <- (1 to 100)) yield { (new ReceiveActor).start }).foreach(_ ! "Hello")
  (for (i <- (1 to 100)) yield { (new ReactActor).start }).foreach(_ ! "Hello")
}