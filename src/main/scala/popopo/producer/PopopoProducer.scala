package popopo.producer

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.kafka.scaladsl.Producer
import akka.kafka.{ProducerMessage, ProducerSettings}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}
import popopo.uti.Logging

import scala.concurrent.duration._
import scala.io.StdIn
import scala.util.{Failure, Success}

object PopopoProducer {
  def settings(system: ActorSystem, bootStrapServers: Seq[String]) = ProducerSettings(system, new ByteArraySerializer, new StringSerializer)
    .withBootstrapServers(bootStrapServers.mkString(","))
    .withCloseTimeout(3.seconds)

  def toRecord[T](topicName: String, stringinizer: T => String, pathThrough: T) = Flow[T]
    .map(stringinizer)
    .map(elem => new ProducerRecord[Array[Byte], String](topicName, 0, null, elem))
    .map(elem => ProducerMessage.Message(elem, pathThrough))
}

object Main extends Logging {
  def main(args: Array[String]) {
    implicit val system = ActorSystem("producers")
    implicit val mat = ActorMaterializer()
    implicit val ec = system.dispatcher

    val producerSettings = PopopoProducer.settings(system, Seq("192.168.33.15:9092"))

    val route = path("events" / Segment) { topicName =>
      post {
        entity(as[String]) { event =>
          val job = Source.single(event)
            .via(PopopoProducer.toRecord(topicName, identity[String], event))
            .via(Producer.flow(producerSettings))
            .runWith(Sink.foreach(x => logger.info(x.toString)))

          onComplete(job) {
            case Success(x) => complete(HttpEntity(ContentTypes.`application/json`, "\":)\""))
            case Failure(x) => complete(HttpEntity(ContentTypes.`application/json`, "\":(\""))
          }
        }
      }
    }

    val bindingFuture = Http().bindAndHandle(route, "localhost", 8080)
    logger.info(s"Server online at http://localhost:8080/\nPress RETURN to stop...")
    StdIn.readLine() // let it run until user presses return
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown when done
  }
}