package com.journal.poc.acceptance

import com.journal.poc.fixture.AcceptanceTestFixture
import com.journal.poc.spec.TestSpec

class JournalAcceptanceSpec extends TestSpec{

  behavior of "A journal"

  it should "receive incoming messages and save them to the inbound journal" in new AcceptanceTestFixture {
    publishMessages(totalMessagesToPublish = 10000, messagesToPublishPerSecond = 1000)
    Thread.sleep(500)
    confirmDatabaseHasAllMessages(numberOfMessages = 10000)
  }
}
