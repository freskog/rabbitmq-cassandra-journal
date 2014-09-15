package com.journal

package object poc {

  case class JournalMessage(correlationId:String, messageId:String, ackTag:Long, payload:Array[Byte])
  case class AcknowledgeBatch(ackTag:Long)
  case object RegisterConsumer

}
