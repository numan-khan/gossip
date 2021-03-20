package com.gossip

import akka.actor.{Actor, Props}
import akka.http.scaladsl.model.HttpMethods
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.Materializer
import com.gossip.config.GossipSettings
import com.gossip.http.client.HttpRequestFactory
import com.gossip.util.RequestMarshallerHelper
import com.gossip.zookeeper.{ZooKeeperLeaderService, ZookeeperLeadership}

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

case object Refresh

class CacheRefresher(
  zooKeeper: ZooKeeperLeaderService,
  requestFactory: HttpRequestFactory,
  cache: Cache,
  settings: GossipSettings
)(implicit mat: Materializer, ex: ExecutionContext)
    extends Actor
    with RequestMarshallerHelper
    with ZookeeperLeadership {
  implicit val zookeeperLeaderLatch: ZooKeeperLeaderService = zooKeeper

  override def receive: Receive = {
    case Refresh =>
      handleFailure("Poll") {
        withLeadership {
          syncCache()
        }
      }
  }

  def syncCache(): Unit =
    handleFailure("syncCache") {
      withLeadership {
        requestFactory
          .createSingleHttpRequest(
            s"${settings.peerEndPoint}${settings.zookeeperInquiryMethod}",
            HttpMethods.GET,
            None,
            None
          )
          .flatMap(
            v =>
              Unmarshal(v.entity).to[String].map { responseStr =>
                createDcMembership(responseStr) match {
                  case Some(membership) =>
                    cache.updateLeader(settings.peerDC, membership.leader)
                  case _ => info("no response from syncCache")
                }
            }
          )
          .recover {
            case NonFatal(t) =>
              val errorMsg = s"Got error from ${settings.peerEndPoint}"
              reportError(errorMsg, new Exception(errorMsg, t))
              List()
          }
      }
    }
}

object CacheRefresher {
  def props(
    zooKeeper: ZooKeeperLeaderService,
    requestFactory: HttpRequestFactory,
    cache: Cache,
    settings: GossipSettings
  )(implicit mat: Materializer, ec: ExecutionContext) =
    Props(
      new CacheRefresher(zooKeeper, requestFactory, cache, settings)(mat, ec)
    )
}
