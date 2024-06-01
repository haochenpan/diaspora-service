#!/bin/bash

# Startup script for Lightsail admin console at http://100.27.155.7
# adapted from https://raw.githubusercontent.com/mikegcoleman/todo/master/lightsail-compose.sh
# and https://stackoverflow.com/a/33370375

# Set the front-end for apt to noninteractive to avoid prompts during package installation
export DEBIAN_FRONTEND=noninteractive

# Update package list and perform a dist-upgrade with forced configuration file overwrite
apt-get update && apt-get -o Dpkg::Options::="--force-confold" dist-upgrade -q -y --force-yes

# Install Docker
curl -sSL https://get.docker.com | sh

# Add the ubuntu user to the docker group
usermod -aG docker ubuntu

# Install Docker Compose
curl -L https://github.com/docker/compose/releases/download/1.21.2/docker-compose-$(uname -s)-$(uname -m) -o /usr/local/bin/docker-compose
chmod +x /usr/local/bin/docker-compose

# Create a directory for Docker configuration
mkdir /srv/docker

# Create a Docker Compose configuration file
cat <<EOF > /srv/docker/docker-compose.yml
version: '3'
services:
  kafka-ui:
    container_name: kafka-ui
    image: provectuslabs/kafka-ui:latest
    ports:
      - "8080:8080"
    environment:
      - DYNAMIC_CONFIG_ENABLED=true
      - AUTH_TYPE=LOGIN_FORM
      - SPRING_SECURITY_USER_NAME=********
      - SPRING_SECURITY_USER_PASSWORD=********
      - AWS_ACCESS_KEY_ID=********************
      - AWS_SECRET_ACCESS_KEY=****************************
    volumes:
      - /srv/docker/config.yml:/etc/kafkaui/dynamic_config.yaml
EOF

# Create a Kafka UI configuration file
cat <<EOF > /srv/docker/config.yml
kafka:
  clusters:
    - name: diaspora
      bootstrapServers: b-1-public.diaspora.fy49oq.c9.kafka.us-east-1.amazonaws.com:9198,b-2-public.diaspora.fy49oq.c9.kafka.us-east-1.amazonaws.com:9198
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

# Schedule a reboot every 3 hours using cron
(crontab -l 2>/dev/null; echo "0 */3 * * * /sbin/reboot") | crontab -