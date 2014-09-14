package com.journal.poc.writer

import akka.actor.{Actor, ActorRef, Props, Stash}
import com.datastax.driver.core._
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.journal.poc.writer.CassandraWriter.SendBatch
import com.journal.poc.{AcknowledgeBatch, JournalMessage, RegisterConsumer}

import scala.collection.JavaConversions._

object CassandraWriter {
  case object SendBatch

  def props(session:Session):Props = Props(new CassandraWriter(session:Session))
}

class CassandraWriter(session:Session) extends Actor with Stash {

  override def receive: Receive = {
    case RegisterConsumer =>
      val consumer = sender()
      context.become(writing(consumer, 0, Nil))
  }

  def writing(consumer:ActorRef, lastReceivedAckTag:Long, statements:List[Statement]):Receive = {
    case JournalMessage(correlationId, messageId, ackTag, payload) =>
      val statement = QueryBuilder.insertInto("journal", "inbound")
        .value("groupId", correlationId)
        .value("messageId", messageId)
        .value("body", payload)
        .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
      context.become(writing(consumer, ackTag, statement :: statements))
    case SendBatch =>
      if(statements.nonEmpty) {
        val batch = new BatchStatement(BatchStatement.Type.UNLOGGED).addAll(statements)
        session.executeAsync(batch)
        consumer ! AcknowledgeBatch(lastReceivedAckTag)
      }
  }
}
