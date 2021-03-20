package com.gossip

import scala.collection.immutable.HashMap

trait Cache {
  def getLeader(dc: String): Option[String]
  def updateLeader(dc: String, leader: String)
}

final class DCLeadersCache extends Cache {
  var cache: HashMap[String, String] = new HashMap[String, String].empty

  def getLeader(dc: String): Option[String] =
    cache.get(dc)

  def updateLeader(dc: String, leader: String): Unit =
    cache += dc -> leader
}
