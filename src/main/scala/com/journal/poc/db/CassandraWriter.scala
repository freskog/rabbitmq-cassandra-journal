package com.journal.poc.db

import java.util.{UUID, Date}

import akka.actor._
import akka.event.Logging
import com.datastax.driver.core._
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.journal.poc.db.CassandraWriter.SendBatch
import com.journal.poc.{AcknowledgeBatch, JournalMessage, RegisterConsumer}

import scala.collection.JavaConversions._
import scala.concurrent.duration._

object CassandraWriter {
  case object SendBatch

  def props(session:Session):Props = Props(new CassandraWriter(session:Session))
}

class CassandraWriter(session:Session) extends Actor with Stash {
  import context.dispatcher

  val log = Logging(context.system, this)

  override def receive: Receive = {
    case RegisterConsumer => becomeWriting(sender())
    case SendBatch =>
    case _ => stash()
  }

  def writing(consumer:ActorRef, lastReceivedAckTag:Long, statements:List[Statement]):Receive = {
    case RegisterConsumer => becomeWriting(sender())
    case SendBatch =>
      acknowledgeBatch(lastReceivedAckTag, statements, consumer)
      context.become(writing(consumer, lastReceivedAckTag, Nil))
    case JournalMessage(correlationId, messageId, ackTag, payload) =>
      context.become(writing(consumer, ackTag, insertStatementFor(correlationId, messageId, payload) :: statements))
  }

  def insertStatementFor(correlationId:String, messageId:UUID, payload:Array[Byte]):Statement = {
    QueryBuilder.insertInto("journal", "inbound")
      .value("correlationId", correlationId)
      .value("messageId", messageId)
      .value("payload", new String(payload, "UTF-8"))
      .value("saved_at", new Date())
      .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
  }

  def acknowledgeBatch(lastReceivedAckTag:Long, statements:List[Statement], consumer:ActorRef) {
    if (statements.nonEmpty) {
      val batch = new BatchStatement(BatchStatement.Type.UNLOGGED).addAll(statements)
      session.execute(batch)
      consumer ! AcknowledgeBatch(lastReceivedAckTag)
      log.debug("Successfully wrote batch, informing consumer")
    }
  }

  def becomeWriting(consumer:ActorRef) {
    context.become(writing(consumer, 0, Nil))
    unstashAll()
    log.debug("Cassandra writer has registered a new consumer")
  }

  context.system.scheduler.schedule(250.millis, 250.millis, self, SendBatch)

}
