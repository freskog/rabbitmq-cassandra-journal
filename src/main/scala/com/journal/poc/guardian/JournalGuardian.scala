package com.journal.poc.guardian

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{AllForOneStrategy, Props, Actor}
import com.datastax.driver.core.Session
import com.journal.poc.amqp.AmqpConsumer
import com.journal.poc.db.CassandraWriter
import com.rabbitmq.client.ConnectionFactory
import scala.concurrent.duration._


object JournalGuardian {
  def props(cf:ConnectionFactory, session:Session):Props =
    Props(new JournalGuardian(cf, session))
}

class JournalGuardian(cf:ConnectionFactory, session:Session) extends Actor {

  val writer    = context.actorOf(CassandraWriter.props(session), "writer")
  val consumer  = AmqpConsumer.props(writer, cf)

  override val supervisorStrategy = AllForOneStrategy(maxNrOfRetries = 5, withinTimeRange = 1.minute) {
    case _: Exception => Restart
  }

  override def receive: Receive = {
    case _ =>
  }
}
