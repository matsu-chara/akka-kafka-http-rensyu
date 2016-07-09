package popopo.consumer

import akka.actor.ActorSystem
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, RequestEntity}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.http.scaladsl.{Http, HttpExt}
import akka.kafka.ConsumerMessage.{CommittableMessage, CommittableOffsetBatch}
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import popopo.uti.Logging

import scala.concurrent.{ExecutionContext, Future}

object PopopoConsumer {
  def settings(system: ActorSystem, bootStrapServers: Seq[String], groupId: String, clientId: String) = ConsumerSettings(system, new ByteArrayDeserializer, new StringDeserializer)
    .withBootstrapServers(bootStrapServers.mkString(","))
    .withGroupId(groupId)
    .withClientId(clientId)
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  def postToMock[K, V](http: HttpExt, uri: String)(implicit mat: ActorMaterializer, ec: ExecutionContext) = Flow[CommittableMessage[K, V]].mapAsync(1) { msg =>
    val resp = for {
        entity <- Marshal(msg.toString).to[RequestEntity]
        response <- http.singleRequest(HttpRequest(method = HttpMethods.POST, uri = uri, entity = entity))
    } yield response
    resp.map(r => (msg, r))
  }
}

object Main extends Logging {
  def main(args: Array[String]) {
    implicit val system = ActorSystem("producers")
    implicit val mat = ActorMaterializer()
    implicit val ec = system.dispatcher
    val http = Http()

    val consumerSettings = PopopoConsumer.settings(system, Seq("192.168.33.15:9092"), groupId = "popopo-test", clientId = "test")

    Consumer.committableSource(consumerSettings, Subscriptions.topics("test"))
      .via(PopopoConsumer.postToMock(http, "http://localhost:8081/events"))
      .map { case (msg, response) =>
        logger.info(msg + " => " + response)
        msg.committableOffset
      }
      .batch(max = 100, first => CommittableOffsetBatch.empty.updated(first)) { (batch, elem) =>
        batch.updated(elem)
      }
      .mapAsync(1)(_.commitScaladsl())
      .runWith(Sink.ignore)
  }
}