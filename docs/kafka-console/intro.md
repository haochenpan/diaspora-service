## Diaspora Admin Console Startup Script

### Overview
This startup script is designed for the Diaspora Admin Console running on the AWS Lightsail instance `kafbat-ui` at [34.201.83.114](http://34.201.83.114/).
It automates the setup of a Docker environment, installs Docker Compose, and configures a Kafbat UI (v1.5.0) service with essential security and environment settings.
Additionally, it establishes a systemd service for managing the Docker Compose application, redirects HTTP traffic from port 80 to port 8080,
and schedules an automated reboot every three days.

### Masked Credentials
The script includes sensitive credentials that have been masked for security purposes:

- `SPRING_SECURITY_USER_NAME`: `********` (Web login username)
- `SPRING_SECURITY_USER_PASSWORD`: `********` (Web login password)
- `AWS_ACCESS_KEY_ID`: `********************` (Used for connecting to MSK)
- `AWS_SECRET_ACCESS_KEY`: `****************************` (Used for connecting to MSK)

### AWS Lightsail Configuration

To deploy this instance, follow these steps:

1. **Create an Instance:**
   - Navigate to [AWS Lightsail](https://lightsail.aws.amazon.com/ls/webapp/create/instance?region=us-east-1).
   - Choose **OS: Linux/Unix → OS only → Ubuntu 24.04 LTS**.
   - Paste the script below into **Add launch script**, so it initializes and configures the instance.
   - Allocate at least **1 GB memory and 2 vCPUs** for optimal performance.
   - Set the **instance name** to `kafbat-ui`.

2. **Assign a Static IP:**
   - Go to **Lightsail → Networking**.
   - Create a static IP in **us-east-1**, naming it `kafbat-ui-static-ip`, and attach it to `kafbat-ui`.

The running instance, created in September 2025, uses Lightsail's WordPress blueprint (Debian 12) instead of Ubuntu, so its login user is `bitnami`.

### Startup Script

```bash
#!/bin/bash

# adapted from https://raw.githubusercontent.com/mikegcoleman/todo/master/lightsail-compose.sh
# and https://stackoverflow.com/a/33370375

# Set the front-end for apt to noninteractive to avoid prompts during package installation
export DEBIAN_FRONTEND=noninteractive

# Update package list and perform a dist-upgrade with forced configuration file overwrite
# apt-get update && apt-get -o Dpkg::Options::="--force-confold" dist-upgrade -q -y --force-yes
apt-get update && apt-get -o Dpkg::Options::="--force-confold" dist-upgrade -q -y --allow-downgrades

# Install Docker
curl -sSL https://get.docker.com | sh

# Add the ubuntu user to the docker group
usermod -aG docker ubuntu

# Install Docker Compose
curl -L https://github.com/docker/compose/releases/download/v2.33.1/docker-compose-$(uname -s)-$(uname -m) -o /usr/local/bin/docker-compose
chmod +x /usr/local/bin/docker-compose

# Create a directory for Docker configuration
mkdir /srv/docker

# Create a Docker Compose configuration file
cat <<EOF > /srv/docker/docker-compose.yml
services:
  kafbat-ui:
    container_name: kafbat-ui
    image: ghcr.io/kafbat/kafka-ui:v1.5.0
    ports:
      - "8080:8080"
    environment:
      - DYNAMIC_CONFIG_ENABLED=true
      # Refresh cluster statistics every 5 minutes, not every 30 s (see Broker CPU credits below)
      - KAFKA_UPDATE_METRICS_RATE_MILLIS=300000
      - AUTH_TYPE=LOGIN_FORM
      - SPRING_SECURITY_USER_NAME=******
      - SPRING_SECURITY_USER_PASSWORD=******
      - AWS_ACCESS_KEY_ID=******
      - AWS_SECRET_ACCESS_KEY=******
    volumes:
      - /srv/docker/config.yml:/etc/kafkaui/dynamic_config.yaml
EOF

# Create a Kafka UI configuration file
cat <<EOF > /srv/docker/config.yml
kafka:
  clusters:
    - name: diaspora
      bootstrapServers: b-1-public.diaspora.jdqn8o.c18.kafka.us-east-1.amazonaws.com:9198,b-2-public.diaspora.jdqn8o.c18.kafka.us-east-1.amazonaws.com:9198
      properties:
        security.protocol: SASL_SSL
        sasl.mechanism: AWS_MSK_IAM
        sasl.client.callback.handler.class: software.amazon.msk.auth.iam.IAMClientCallbackHandler
        sasl.jaas.config: software.amazon.msk.auth.iam.IAMLoginModule required awsProfileName="default";
EOF

# Download the systemd service file for Docker Compose application
curl -o /etc/systemd/system/docker-compose-app.service https://raw.githubusercontent.com/zahedahmed/todo/master/docker-compose-app.service

# Enable the Docker Compose application service
systemctl enable docker-compose-app

# Start the Docker Compose application in detached mode
docker-compose -f /srv/docker/docker-compose.yml up -d

# Redirect port 80 to port 8080 using iptables
iptables -t nat -A PREROUTING -p tcp --dport 80 -j REDIRECT --to-port 8080

# Install iptables-persistent to save iptables rules
apt-get install -y iptables-persistent
netfilter-persistent save

# Schedule a reboot every 3 days at 4:30 AM
(crontab -l 2>/dev/null; echo "30 4 */3 * * /sbin/reboot") | crontab -

```

### Upgrading Kafbat UI

The instance runs the image tag pinned in `/srv/docker/docker-compose.yml`.
Docker does not pull a newer image by itself, and the reboot every three days restarts the same image.
To move to a new [Kafbat UI release](https://github.com/kafbat/kafka-ui/releases):

1. Connect over SSH as `bitnami`, from the Lightsail console or with a temporary key from
   `aws lightsail get-instance-access-details --instance-name kafbat-ui --protocol ssh`.
2. Back up the compose file, change the tag on its `image:` line, then pull and restart:
   ```bash
   cd /srv/docker
   sudo cp -p docker-compose.yml docker-compose.yml.bak
   sudoedit docker-compose.yml
   sudo docker-compose pull
   sudo docker-compose up -d
   ```
3. Check that `sudo docker ps` shows the new tag and that the login page loads.
   Kafbat UI takes about two minutes to start on this instance.

To roll back, restore `docker-compose.yml.bak` and run the last two commands again.

### Broker CPU credits

The `diaspora` cluster's brokers are `kafka.t3.small` instances, which run on CPU credits.
Each broker earns 24 credits an hour and holds at most 576; a broker that runs out is throttled to its baseline.

Kafbat UI refreshes its cluster statistics on a timer.
Each refresh reads every topic and its configuration, the earliest and latest offsets of every partition,
every consumer group and its offsets, and each broker's log directories.
In October 2026, with the default refresh of 30 s, the console's load spent broker 1's credits,
and topic creation and deletion stalled until the instance was stopped.
`KAFKA_UPDATE_METRICS_RATE_MILLIS=300000` sets the refresh to 5 minutes,
so the topic sizes and consumer lag the console shows can be up to 5 minutes old.

After any change to the console, watch each broker's `CPUCreditBalance` in CloudWatch
(namespace `AWS/Kafka`, dimensions `Cluster Name` and `Broker ID`).
If it keeps falling, stop the instance; its static IP keeps the address for the next start.
