package com.gossip

import akka.actor.{Actor, Props}
import akka.stream.Materializer
import com.gossip.config.GossipSettings
import com.gossip.http.client.HttpRequestFactory
import com.gossip.model.GossipMessage
import com.gossip.util.AppLoggerReporter
import com.gossip.zookeeper.{ZooKeeperLeaderService, ZookeeperLeadership}
import org.joda.time.{DateTime, Period}

case object CheckForActive
case class GossipReceive(message: GossipMessage)
case class GossipWrapper(msg: GossipMessage, timeStamp: DateTime)

class ActivePassiveMonitor(zooKeeper: ZooKeeperLeaderService,
                           httpRequestFactory: HttpRequestFactory,
                           settings: GossipSettings)
    extends Actor
    with AppLoggerReporter
    with ZookeeperLeadership {
  implicit val zookeeperLeaderLatch: ZooKeeperLeaderService = zooKeeper
  implicit val gSetting: GossipSettings = settings

  var lastGossipMessage: Option[GossipWrapper] = None
  IsActiveDc.value = settings.isActive

  override def receive: Receive = {
    case CheckForActive =>
      handleFailure("CheckForActive") {
        withLeadership {
          onPassiveDC {
            checkForActive()
          }
        }
      }
    case GossipReceive(message) =>
      handleFailure("GossipReceive") {
        withLeadership {
          logGossipReceive(GossipWrapper(message, DateTime.now()))
        }
      }
  }

  def logGossipReceive(gossipMessage: GossipWrapper): Unit = {
    lastGossipMessage = Some(gossipMessage)

    if (gossipMessage.msg.leader && !settings.isActive && IsActiveDc.value) {
      //step down from being Active DC as the other DC is live now
      IsActiveDc.value = false

      info("Step down as Active DC as the actual DC is live now....")
    }

    info("Am I Active DC ?? " + IsActiveDc.value)
  }

  def checkForActive(): Unit = {
    lastGossipMessage match {
      case Some(message) =>
        val duration = new Period(message.timeStamp, DateTime.now())
        if (duration.getMinutes >= settings.failoverDuration && !IsActiveDc.value) {

          IsActiveDc.value = true
        }
      case _ => info(s"has not received any gossip message")
    }

    info("Am I Active DC ?? " + IsActiveDc.value)
  }

}

object ActivePassiveMonitor {
  def props(zooKeeper: ZooKeeperLeaderService,
            httpClient: HttpRequestFactory,
            cache: Cache,
            settings: GossipSettings)(implicit mat: Materializer) =
    Props(new ActivePassiveMonitor(zooKeeper, httpClient, settings))
}
