package com.journal.poc.amqp

import java.util.UUID

import akka.actor.{Actor, ActorRef, Props}
import akka.event.Logging
import com.journal.poc.amqp.AmqpConsumer.MessageReceived
import com.journal.poc.{RegisterConsumer, AcknowledgeBatch, JournalMessage}
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.{ConnectionFactory, Consumer, Envelope, ShutdownSignalException}

object AmqpConsumer {

  case class MessageReceived(correlationId:String, messageId:UUID, ackTag:Long, payload:Array[Byte])

  def props(writer:ActorRef, cf:ConnectionFactory):Props = Props(new AmqpConsumer(writer, cf))
}

class AmqpConsumer(writer:ActorRef, cf:ConnectionFactory) extends Actor with Consumer {

  val log = Logging(context.system, this)

  override def receive: Receive = {
    case AcknowledgeBatch(ackTag) =>
      log.debug(s"performing BatchAck to rabbit with deliveryTag = $ackTag")
      channel.basicAck(ackTag, true)
    case MessageReceived(correlationId, messageId, ackTag, payload) =>
      log.debug(s"message received with deliveryTag = $ackTag")
      writer ! JournalMessage(correlationId, messageId, ackTag, payload)
  }

  override def handleConsumeOk(p1: String): Unit = {}
  override def handleRecoverOk(p1: String): Unit = {}
  override def handleCancel(p1: String): Unit = throw new IllegalStateException("Consumer cancelled!")
  override def handleCancelOk(p1: String): Unit = {}
  override def handleShutdownSignal(p1: String, p2: ShutdownSignalException): Unit = throw new IllegalStateException("Consumer shutdown!")
  override def handleDelivery(p1: String, p2: Envelope, p3: BasicProperties, p4: Array[Byte]): Unit = {
    self ! MessageReceived(p3.getCorrelationId, UUID.fromString(p3.getMessageId), p2.getDeliveryTag, p4)
  }

  cf.setAutomaticRecoveryEnabled(false)
  val connection = cf.newConnection()
  val channel = connection.createChannel()
  channel.exchangeDeclare("inboundExchange", "fanout", true)
  channel.queueDeclare("inboundJournal", false, false, false, null)
  channel.queueBind("inboundJournal","inboundExchange","")
  channel.basicConsume("inboundJournal", false, this)
  channel.basicQos(1000, true)
  writer ! RegisterConsumer
}
