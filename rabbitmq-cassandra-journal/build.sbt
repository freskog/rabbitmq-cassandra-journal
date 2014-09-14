name := "rabbitmq-cassandra-journal"

version := "1.0"

scalaVersion := "2.11.2"

libraryDependencies ++= Seq(
  "com.rabbitmq" % "amqp-client" % "3.3.5",
  "com.datastax.cassandra" % "cassandra-driver-core" % "2.1.0",
  "com.typesafe.akka" %% "akka-actor" % "2.3.6",
  "com.typesafe" % "config" % "1.2.1",
  "ch.qos.logback" % "logback-classic" % "1.1.2",
  "org.cassandraunit" % "cassandra-unit" % "2.0.2.1" % "test",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  "org.scalautils" %% "scalautils" % "2.1.7" % "test"
)