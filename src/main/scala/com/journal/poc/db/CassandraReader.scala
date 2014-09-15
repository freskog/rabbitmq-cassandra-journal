package com.journal.poc.db

import java.util.UUID

import akka.actor.Actor
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.{ConsistencyLevel, Session}
import com.journal.poc.{ReadRowUntil, ReplayedMessage}

import scala.collection.JavaConversions._

class CassandraReader(session:Session) extends Actor {

  override def receive: Receive = {
    case ReadRowUntil(correlationId, messageId) => sender ! readUntil(correlationId, messageId)
  }

  def readUntil(correlationId:String, messageId:UUID): List[ReplayedMessage] = {
    val readStatement = QueryBuilder
      .select()
      .all()
      .from("journal","inbound")
      .where(QueryBuilder.eq("correlationId",correlationId))
      .and(QueryBuilder.lte("messageId",messageId))
      .setFetchSize(1000)
      .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
    for( row <- session.execute(readStatement).iterator().toList) yield {
      ReplayedMessage(row.getString("correlationId"), row.getUUID("messageId"), row.getBytes("payload").array())
    }
  }

}
