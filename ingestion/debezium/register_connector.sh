#!/bin/bash
# Wait for Kafka Connect to be ready
echo "Waiting for Kafka Connect to start..."
sleep 15

echo "Registering Debezium Postgres Connector..."
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
    http://localhost:8083/connectors/ -d @- <<EOF
{
  "name": "kinetix-orders-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "replicator",
    "database.password": "replicator_password",
    "database.dbname": "kinetix_db",
    "database.server.name": "kinetix_db",
    "topic.prefix": "dbserver1",
    "table.include.list": "public.orders",
    "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
    "schema.history.internal.kafka.topic": "schema-changes.orders",
    "plugin.path": "/kafka/connect"
  }
}
EOF

echo "\nConnector registered. Checking status..."
curl -s http://localhost:8083/connectors/kinetix-orders-connector/status | jq .