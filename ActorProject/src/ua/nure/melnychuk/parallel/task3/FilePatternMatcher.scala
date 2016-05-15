package ua.nure.melnychuk.parallel.task3

import akka.actor.ActorRef
import akka.actor.Actor
import java.time.Duration
import java.io.File
import akka.actor.ActorSystem
import akka.actor.Props
import scala.util.matching.Regex
import scala.io.Source
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet

object FilePatternMatcher extends App {

  findMatches("folder", "([0-9])".r)

  sealed trait Message
  case object Match extends Message
  case class Work(file: File, regexp: Regex) extends Message
  case class Result(matches: List[String], filename: String) extends Message
  case object Increment extends Message

  class FileCrawler(collector: ActorRef, directory: String, regexp: Regex) extends Actor {
    def receive = {
      case Match => {
        crawlFiles(directory).foreach { f => context.actorOf(Props(classOf[Worker], collector)) ! Work(f, regexp) }
      }
    }

    def crawlFiles(directory: String): ListBuffer[File] = {
      var files = new ListBuffer[File]
      new File(directory).listFiles.foreach {
        (item =>
          if (item.isDirectory) {
            files ++= crawlFiles(item.getAbsolutePath)
          } else if (item.isFile()) {
            files += item
            collector ! Increment
          })
      }
      files
    }
  }

  class Collector extends Actor {
    var amountOfMatches: Int = _
    var amountOfFiles: Int = _
    var counts: Int = _
    val matched = HashMap[String, HashSet[String]]()

    def receive = {
      case Result(matches, filename) => {
        counts += 1
        amountOfMatches += matches.size
        matches.foreach(matched.getOrElseUpdate(_, new HashSet[String]()).+=(filename))
        if (amountOfFiles == counts) {
          println("%s matches in %s files".format(amountOfMatches, amountOfFiles))
          matched.foreach(m => println("%s was found in files: %s;".format(m._1, m._2.mkString(", "))))
          context.system.terminate()
        }
      }
      case Increment => amountOfFiles += 1
    }
  }

  class Worker(collector: ActorRef) extends Actor {
    def receive = {
      case Work(file, regexp) => {
        collector ! Result(regexp.findAllIn(Source.fromFile(file, "ISO-8859-1").getLines.mkString).toList, file.getName)
      }
    }
  }

  def findMatches(directory: String, regexp: Regex) = {
    val system = ActorSystem("FilePatternMatcher")
    val collector = system.actorOf(Props[Collector], name = "collector")
    val crawler = system.actorOf(Props(classOf[FileCrawler], collector, directory, regexp), name = "crawler")
    crawler ! Match
  }

}