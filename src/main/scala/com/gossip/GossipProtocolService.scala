package com.gossip

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem}
import akka.stream.ActorMaterializer
import com.gossip.config.GossipSettings
import com.gossip.http.client.HttpRequestFactoryImpl
import com.gossip.model.GossipMessage
import com.gossip.util.AppLoggerReporter
import com.gossip.zookeeper.ZooKeeperLeaderService
import com.typesafe.config.Config

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

class GossipProtocolService(config: Config, zkLeader: ZooKeeperLeaderService)(
  implicit actorSystem: ActorSystem,
  actorMaterializer: ActorMaterializer,
  executionContext: ExecutionContext
) extends AppLoggerReporter {
  var cacheUpdateWorker: Option[ActorRef] = None
  var gossipWorker: Option[ActorRef] = None
  var activePassiveMonitor: Option[ActorRef] = None

  def run() {
    val gossipSettings = GossipSettings(config)
    val httpClientImpl = new HttpRequestFactoryImpl()
    val leaderCache = new DCLeadersCache()

    cacheUpdateWorker = Some(
      actorSystem.actorOf(
        CacheRefresher
          .props(zkLeader, httpClientImpl, leaderCache, gossipSettings)
      )
    )

    gossipWorker = Some(
      actorSystem.actorOf(
        GossipWorker
          .props(zkLeader, httpClientImpl, leaderCache, gossipSettings)
      )
    )

    activePassiveMonitor = Some(
      actorSystem.actorOf(
        ActivePassiveMonitor
          .props(zkLeader, httpClientImpl, leaderCache, gossipSettings)
      )
    )

    startHeartBeatScheduler()

    def startHeartBeatScheduler() = {
      gossipWorker match {
        case Some(worker) =>
          actorSystem.scheduler.schedule(
            FiniteDuration.apply(1, TimeUnit.MINUTES),
            FiniteDuration.apply(10, TimeUnit.SECONDS),
            worker,
            Gossip
          )
        case _ => info("gossipWorker is not set")
      }

      cacheUpdateWorker match {
        case Some(worker) =>
          actorSystem.scheduler.schedule(
            FiniteDuration.apply(1, TimeUnit.MINUTES),
            FiniteDuration.apply(1, TimeUnit.MINUTES),
            worker,
            Refresh
          )
        case _ => info("cacheUpdateWorker is not set")

      }

      activePassiveMonitor match {
        case Some(worker) =>
          actorSystem.scheduler.schedule(
            FiniteDuration.apply(1, TimeUnit.MINUTES),
            FiniteDuration.apply(1, TimeUnit.MINUTES),
            worker,
            CheckForActive
          )
        case _ => info("activePassiveMonitor is not set")

      }
    }
  }

  def onGossipMessage(msg: GossipMessage): Unit =
    activePassiveMonitor match {
      case Some(handler) => handler ! GossipReceive(msg)
      case _             => info("no handler for gossiping")
    }
}
