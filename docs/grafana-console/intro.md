
# Commands to setup the Grafana Console on LightSail (Ubuntu 22.04)
### Masked Credentials

replace them below with appropriate credentials:

- `AWS_ACCESS_KEY_ID`: `********************`
- `AWS_SECRET_ACCESS_KEY`: `****************************`

The keys need to have IAM access to S3 for Thanos; For details see [this article](https://aws.amazon.com/blogs/opensource/improving-ha-and-long-term-storage-for-prometheus-using-thanos-on-eks-with-s3/).

### 1. System updates and docker install
```bash
sudo su

# Set the front-end for apt to noninteractive to avoid prompts during package installation
export DEBIAN_FRONTEND=noninteractive

# Update package list and perform a dist-upgrade with forced configuration file overwrite
apt-get update && apt-get -o Dpkg::Options::="--force-confold" dist-upgrade -q -y --allow-downgrades

# Install Docker
curl -sSL https://get.docker.com | sh

# Add the ubuntu user to the docker group
usermod -aG docker ubuntu

# Install Docker Compose
curl -L https://github.com/docker/compose/releases/download/1.21.2/docker-compose-$(uname -s)-$(uname -m) -o /usr/local/bin/docker-compose
chmod +x /usr/local/bin/docker-compose

# Download the systemd service file for Docker Compose application
curl -o /etc/systemd/system/docker-compose-app.service https://raw.githubusercontent.com/zahedahmed/todo/master/docker-compose-app.service

# Enable the Docker Compose application service
systemctl enable docker-compose-app

# Install the AWS CLI
apt-get install -y aws-cli
```

TODO: make sure docker is installed; I have to run `curl -sSL https://get.docker.com | sh` again after this block of code.

### 2. Install and config Prometheus and Thanos

```bash

# Set AWS access keys
echo "export AWS_ACCESS_KEY_ID=********************" >> /root/.bashrc
echo "export AWS_SECRET_ACCESS_KEY=****************************" >> /root/.bashrc
source /root/.bashrc

# Navigate to the home directory
cd /root

# Download Prometheus and Thanos binaries
wget https://github.com/prometheus/prometheus/releases/download/v2.53.0/prometheus-2.53.0.linux-amd64.tar.gz
wget https://github.com/thanos-io/thanos/releases/download/v0.35.1/thanos-0.35.1.linux-amd64.tar.gz

# Extract the downloaded archives
tar -xvzf prometheus-2.53.0.linux-amd64.tar.gz
tar -xvzf thanos-0.35.1.linux-amd64.tar.gz -C prometheus-2.53.0.linux-amd64 --strip-components=1

# Navigate to the Prometheus directory
cd prometheus-2.53.0.linux-amd64

# Create Prometheus configuration file
cat <<EOF > prometheus.yml
global:
  scrape_interval: 1m
  evaluation_interval: 1m
  external_labels:
    region: us-east-1
    monitor: 'diaspora MSK'
    replica: A

scrape_configs:
  - job_name: "prometheus"
    static_configs:
      - targets: ["localhost:9090"]

  - job_name: 'brokers'
    file_sd_configs:
    - files:
      - 'targets.json'
EOF

# Create targets file for Prometheus
cat <<EOF > targets.json
[
    {
      "labels": {
        "job": "jmx"
      },
      "targets": [
        "b-2-public.diaspora.fy49oq.c9.kafka.us-east-1.amazonaws.com:11001",
        "b-1-public.diaspora.fy49oq.c9.kafka.us-east-1.amazonaws.com:11001"
      ]
    },
    {
      "labels": {
        "job": "node"
      },
      "targets": [
        "b-2-public.diaspora.fy49oq.c9.kafka.us-east-1.amazonaws.com:11002",
        "b-1-public.diaspora.fy49oq.c9.kafka.us-east-1.amazonaws.com:11002"
      ]
    }
  ]
EOF

# Create Thanos bucket configuration
cat <<EOF > bucket_config.yaml
type: S3
config:
  bucket: "diaspora-prometheus-prod"
  endpoint: "s3.us-east-1.amazonaws.com"
  region: "us-east-1"
  aws_sdk_auth: true
EOF

# Navigate back to the home directory
cd /root

# Create and run a shell script to start Prometheus and Thanos services
cat <<EOF > diaspora-monitoring.sh
cd prometheus-2.53.0.linux-amd64
echo "starting prometheus"
nohup ./prometheus \
    --config.file=prometheus.yml \
    --web.listen-address=0.0.0.0:9090 \
    --storage.tsdb.max-block-duration=2h \
    --storage.tsdb.min-block-duration=2h \
    --storage.tsdb.wal-compression \
    --storage.tsdb.retention.time=2h \
    --web.enable-lifecycle \
    --web.enable-admin-api &> prometheus.out &
sleep 2

echo "starting thanos sidecar"
nohup ./thanos sidecar \
    --objstore.config-file bucket_config.yaml \
    --prometheus.url   http://localhost:9090 \
    --grpc-address 0.0.0.0:10901 \
    --http-address 0.0.0.0:10902 &> thanos-sidecar.out &
sleep 2

echo "starting thanos store"
nohup ./thanos store  \
    --objstore.config-file bucket_config.yaml \
    --grpc-address 0.0.0.0:10903 \
    --http-address 0.0.0.0:10904 &> thanos-store.out &
sleep 2

echo "starting thanos query"
nohup ./thanos query \
    --grpc-address 0.0.0.0:10905 \
    --http-address 0.0.0.0:10906 \
    --endpoint 0.0.0.0:10901 \
    --endpoint 0.0.0.0:10903 &> thanos-query.out &

echo "starting grafana"
docker start grafana # an idempotent start cmd
cd ..
EOF

chmod +x diaspora-monitoring.sh
```

### 3.Start Grafana, Prometheus, and Thanos
```bash
# Start Grafana in a Docker container
docker run -d -p 3000:3000 --name=grafana grafana/grafana-enterprise

# Start Prometheus and Thanos
./diaspora-monitoring.sh
```

### 4.Configure Grafana port

AWS LightSail exposes port 80 by default, other http and gRPC ports used by Prometheus and Thanos are blocked.

```bash
# Redirect port 80 to port 8080 using iptables
iptables -t nat -A PREROUTING -p tcp --dport 80 -j REDIRECT --to-port 3000

# Install iptables-persistent to save iptables rules
apt-get install -y iptables-persistent
netfilter-persistent save

# Add cron job to restart the monitoring script at every reboot
# (crontab -l 2>/dev/null; echo "@reboot /root/diaspora-monitoring.sh") | crontab -

```
TODO: test the cron job.

Grafana default credential: admin admin

Add a new datasource with Prometheus server URL set to http://172.17.0.1:10906. The bridge address can be found using `sudo docker network inspect bridge`
