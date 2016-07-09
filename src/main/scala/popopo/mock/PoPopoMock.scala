package popopo.mock

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import popopo.uti.Logging

import scala.io.StdIn

object Main extends Logging {

  def main(args: Array[String]) {
    implicit val system = ActorSystem("producers")
    implicit val mat = ActorMaterializer()
    implicit val ec = system.dispatcher

    val route = path("events") {
      post {
        entity(as[String]) { event =>
          logger.info(event)
          Thread.sleep(500)
          complete(HttpEntity(ContentTypes.`application/json`, "\":)\""))
        }
      }
    }

    val bindingFuture = Http().bindAndHandle(route, "localhost", 8081)
    logger.info(s"Server online at http://localhost:8081/\nPress RETURN to stop...")
    StdIn.readLine() // let it run until user presses return
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown when done
  }
}
