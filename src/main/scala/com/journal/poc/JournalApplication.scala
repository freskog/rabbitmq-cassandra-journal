package com.journal.poc

import akka.actor.ActorSystem
import com.datastax.driver.core.Cluster
import com.journal.poc.guardian.JournalGuardian
import com.rabbitmq.client.ConnectionFactory

object JournalApplication extends App {

  val cluster = Cluster
    .builder()
    .addContactPoint("localhost")
    .build()

  val connectionFactory = new ConnectionFactory()

  val actorSystem = ActorSystem("Journal-App")

  val guardian = actorSystem.actorOf(JournalGuardian.props(connectionFactory, cluster.connect()))

}
