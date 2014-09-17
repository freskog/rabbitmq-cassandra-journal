package com.journal.poc.fixture

import java.util.{Date, UUID}

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.utils.UUIDs
import com.rabbitmq.client.{AMQP, ConnectionFactory}
import org.scalatest.DiagrammedAssertions

class AcceptanceTestFixture extends DiagrammedAssertions {

  private case class AmqpMessage(correlationId:String, messageId:UUID, payload:Array[Byte])

  val cf = new ConnectionFactory()
  val connection = cf.newConnection()
  val channel = connection.createChannel()

  val cluster = Cluster.builder().addContactPoint("localhost").build()
  val session = cluster.connect("journal")

  private val messageGenerator = Iterator.from( 1 ).map { i => AmqpMessage(convertToCorrelationId(i), UUIDs.startOf(1410964472794L+i), convertIntToByteArray(i))}

  private def convertToCorrelationId(i: Int): String = {
    (i % 100).toString
  }

  private def convertIntToByteArray(i: Int): Array[Byte] = {
    i.toString.toCharArray.map(_.toByte)
  }

  private def publish(message:AmqpMessage):Unit = {
    val props = new AMQP.BasicProperties.Builder()
      .`type`("some-message-type")
      .appId("acceptance test publisher")
      .contentEncoding("UTF-8")
      .contentType("text/plain")
      .correlationId(message.correlationId)
      .deliveryMode(1)
      .messageId(message.messageId.toString)
      .timestamp(new Date())
      .build()
    channel.basicPublish("inboundExchange", "", props, message.payload)
  }

  def publishMessages(totalMessagesToPublish:Int, messagesToPublishPerSecond:Int) = {
    var lastThrottleAt = System.currentTimeMillis()
    var messagesSentInLastSecond = 0
    messageGenerator.take(totalMessagesToPublish).foreach { message =>
      if(messagesSentInLastSecond > messagesToPublishPerSecond) {
        val untilNextSecond = 1000 - (System.currentTimeMillis() - lastThrottleAt)
        Thread.sleep(untilNextSecond)
        lastThrottleAt = System.currentTimeMillis()
        messagesSentInLastSecond = 0
      }
      publish(message)
      messagesSentInLastSecond += 1
    }
  }

  def confirmDatabaseHasAllMessages(numberOfMessages:Int) = {
    val numberOfRowsToRead = Math.min(100, numberOfMessages)

    val actualCountPerRow = for(correlationId <- 0.until(numberOfRowsToRead)) yield
      session.execute(s"select count(*) from journal.inbound where correlationId='$correlationId'").one().getLong(0)

    assert(actualCountPerRow.reduce(_+_) == numberOfMessages, "Not all message found in journal!")
  }

  session.execute("drop table journal.inbound")
  session.execute("""create table if not exists journal.inbound(
                    |    correlationId text,
                    |    messageId timeuuid,
                    |    payload text,
                    |    saved_at timestamp,
                    |    PRIMARY KEY(correlationId, messageId)
                    |    ) WITH CLUSTERING ORDER BY (messageId ASC)""".stripMargin)

}
