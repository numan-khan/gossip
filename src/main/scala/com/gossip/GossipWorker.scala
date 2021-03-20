package com.gossip

import akka.actor.{Actor, Props}
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpMethods}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.Materializer
import com.gossip.config.GossipSettings
import com.gossip.http.client.HttpRequestFactory
import com.gossip.zookeeper.{ZooKeeperLeaderService, ZookeeperLeadership}
import org.joda.time.DateTime

import scala.concurrent.ExecutionContextExecutor
import scala.util.control.NonFatal

case object Gossip

class GossipWorker(zooKeeper: ZooKeeperLeaderService,
                   requestFactory: HttpRequestFactory,
                   cache: Cache,
                   settings: GossipSettings)(implicit mat: Materializer)
    extends Actor
    with ZookeeperLeadership {
  implicit val zookeeperLeaderLatch: ZooKeeperLeaderService = zooKeeper
  implicit lazy val executionContext: ExecutionContextExecutor =
    context.dispatcher
  def gossipMessage(serverName: String) =
    s"""{"serverName": "$serverName", "serverAddress": "$serverName", "queuePointer": "sdfsdf", "dateTime": "${DateTime
      .now()}", "leader": ${zookeeperLeaderLatch.hasLeadership}}"""

  override def receive: Receive = {
    case Gossip =>
      handleFailure("Heartbeat") {
        withLeadership {
          sendGossipMessage()
        }
      }
  }

  private def getURI(machineName: String, method: String) =
    "http://" + machineName + ":" + settings.peerPort + "/inventory-manager/gossip/" + method

  def sendGossipMessage(): Any =
    cache.getLeader(settings.peerDC) match {
      case Some(leader) =>
        requestFactory
          .createSingleHttpRequest(
            getURI(leader, settings.gossipMethod),
            HttpMethods.POST,
            None,
            Some(
              HttpEntity(
                contentType = ContentTypes.`application/json`,
                string = gossipMessage(leader)
              )
            )
          )
          .flatMap(
            v =>
              Unmarshal(v.entity).to[String].map { responseStr =>
                info(s"DiscoveryService ResponseMsg: $responseStr")
            }
          )
          .recover {
            case NonFatal(t) =>
              val errorMsg = s"Got error from $leader"
              reportError(errorMsg, new Exception(errorMsg, t))
              List()
          }
      case _ => info(s"Peer DC leader is None")
    }
}

object GossipWorker {
  def props(zooKeeper: ZooKeeperLeaderService,
            requestFactory: HttpRequestFactory,
            cache: Cache,
            settings: GossipSettings)(implicit mat: Materializer): Props =
    Props(new GossipWorker(zooKeeper, requestFactory, cache, settings)(mat))
}
