# Monitoring

An operator can watch the Lightsail container service and the MSK
brokers. Every command on this page only reads; the
[runbook](5-runbook.md) says what to do when something looks wrong.

## The Lightsail service

The service runs on Lightsail's `micro` power (0.25 vCPU and 1 GB of
memory), and Lightsail records its CPU and memory use as percentages of
that. Each open consumer instance holds a Kafka consumer in the service's
memory. The Lightsail console shows these metrics on the service's page.
From the CLI, memory use in 5-minute periods:

```bash
aws lightsail get-container-service-metric-data --region us-east-1 \
    --service-name diaspora-web-service --metric-name MemoryUtilization \
    --start-time <start-time> --end-time <end-time> \
    --period 300 --statistics Average Maximum
```

The times are in ISO 8601, such as `2026-10-26T06:00:00Z`.
`--metric-name CPUUtilization` gives the CPU use.

The container's log holds gunicorn's and the service's output for the last
three days ([Read the logs](5-runbook.md#read-the-logs)).
`aws lightsail get-container-services` shows whether the service is
running and which deployment is active
([Check a deploy](3-deploying.md#check-a-deploy)).

On GitHub Actions, the `deployment` workflow's runs show each deploy, and
the `tests` workflow's nightly runs show the live tests. The live tests
run against the AWS account and the MSK cluster, not against the deployed
service.

## The MSK brokers

Each broker's `CPUCreditBalance` metric in CloudWatch (namespace
`AWS/Kafka`, dimensions `Cluster Name` and `Broker ID`) is the number of
CPU credits it has left. A broker that runs out is throttled
([Broker CPU credits](5-runbook.md#broker-cpu-credits)).

## The Kafka console

The [Diaspora Admin Console](kafka-console/intro.md) is a Kafbat UI
instance that shows the cluster's topics, consumer groups and consumer
lag. Its statistics can be up to 5 minutes old.
