# Kafka Manager Consumer Lag exporter

This is an exporter for Prometheus to get instrumentation data for [KafkaManager](https://github.com/yahoo/kafka-manager) Consumers

## Build and run

```bash
go build .
./kafka-manager-consumer-lag-exporter -http.port :8080 -kafka-manager http://localhost:9000 -user user -pass pass
```


### Flags

Name                            | Description
--------------------------------|------------
http.port                       | Address to listen on for web interface and telemetry.
kafka-manager                   | [URL](#kafka-manager) to an KafkaManager.
user                            | Username
pass                            | Password
alsologtostderr                 |
logtostderr                     | Log to Stderr
