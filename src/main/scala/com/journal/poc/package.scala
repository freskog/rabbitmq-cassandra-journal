package com.journal

import java.util.UUID

package object poc {

  case class ReplayedMessage(correlationId:String, messageId:UUID, payload:Array[Byte])
  case class ReadRowUntil(correlationId:String, messageId:UUID)

  case class JournalMessage(correlationId:String, messageId:UUID, ackTag:Long, payload:Array[Byte])
  case class AcknowledgeBatch(ackTag:Long)
  case object RegisterConsumer

}
