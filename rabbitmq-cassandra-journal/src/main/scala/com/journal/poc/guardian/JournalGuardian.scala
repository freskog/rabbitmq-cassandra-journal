package com.journal.poc.guardian

import akka.actor.{Props, Actor}
import com.datastax.driver.core.Session
import com.journal.poc.consumer.AmqpConsumer
import com.journal.poc.writer.CassandraWriter
import com.rabbitmq.client.ConnectionFactory

object JournalGuardian {
  def props(cf:ConnectionFactory, session:Session):Props =
    Props(new JournalGuardian(cf, session))
}

class JournalGuardian(cf:ConnectionFactory, session:Session) extends Actor {

  val writer    = CassandraWriter.props(session)
  val consumer  = AmqpConsumer.props(writer, cf)

  override def receive: Receive = {
    case _ =>
  }
}
