# Kafka Connect no longer ships the JDBC connector, so install it into the image.
# The plugin bundles the Postgres JDBC driver.
FROM confluentinc/cp-kafka-connect:8.2.4
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.9.9
