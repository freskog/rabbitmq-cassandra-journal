package com.journal.poc.consumer

import akka.actor.{ActorRef, Stash, Props, Actor}
import com.journal.poc.{JournalMessage, AcknowledgeBatch, WriterInitialized}
import com.journal.poc.consumer.AmqpConsumer.MessageReceived
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.{Envelope, ShutdownSignalException, Consumer, ConnectionFactory}

object AmqpConsumer {

  case class MessageReceived(correlationId:String, messageId:String, ackTag:Long, payload:Array[Byte])

  def props(cf:ConnectionFactory):Props = Props(new AmqpConsumer(cf))
}

class AmqpConsumer(writer:ActorRef, cf:ConnectionFactory) extends Actor with Consumer {

  override def receive: Receive = {
    case AcknowledgeBatch(ackTag) => channel.basicAck(ackTag, true)
    case MessageReceived(correlationId, messageId, ackTag, payload) =>
      writer ! JournalMessage(correlationId, messageId, ackTag, payload)
  }

  override def handleConsumeOk(p1: String): Unit = {}
  override def handleRecoverOk(p1: String): Unit = {}
  override def handleCancel(p1: String): Unit = throw new IllegalStateException("Consumer cancelled!")
  override def handleCancelOk(p1: String): Unit = {}
  override def handleShutdownSignal(p1: String, p2: ShutdownSignalException): Unit = throw new IllegalStateException("Consumer shutdown!")
  override def handleDelivery(p1: String, p2: Envelope, p3: BasicProperties, p4: Array[Byte]): Unit = {
    self ! MessageReceived(p3.getCorrelationId, p3.getMessageId, p2.getDeliveryTag, p4)
  }

  cf.setAutomaticRecoveryEnabled(false)
  val connection = cf.newConnection()
  val channel = connection.createChannel()
  channel.exchangeDeclare("inboundExchange", "fanout", true)
  channel.queueDeclare("inboundJournal", false, false, false, null)
  channel.queueBind("inboundJournal","inboundExchange","")
  channel.basicConsume("inboundJournal", false, this)
}
