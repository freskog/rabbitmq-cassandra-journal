
create keyspace if not exists journal WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '1'
};

create table if not exists journal.inbound(
    correlationId text,
    messageId timeuuid,
    payload text,
    saved_at timestamp,
    PRIMARY KEY(correlationId, messageId)
    ) WITH CLUSTERING ORDER BY (messageId ASC);

