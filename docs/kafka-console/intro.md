## Diaspora Admin Console Startup Script

### Overview
This startup script is designed for the Diaspora Admin Console running on an AWS Lightsail instance at [184.73.61.163](http://184.73.61.163/).
It automates the setup of a Docker environment, installs Docker Compose, and configures a Kafka UI service with essential security and environment settings.
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
   - Use the script provided below to initialize and configure the instance.
   - Allocate at least **1 GB memory and 2 vCPUs** for optimal performance.
   - Set the **instance name** to `kafka-ui`.

2. **Assign a Static IP:**
   - Go to **Lightsail → Networking**.
   - Create a static IP in **us-east-1**, naming it `kafka-ui-ip`.

3. **Launch the Startup Script:**

Lightsail -> Networking -> Create  static IP : us-east-1, name:kafka-ui-ip

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
    image: ghcr.io/kafbat/kafka-ui:latest
    ports:
      - "8080:8080"
    environment:
      - DYNAMIC_CONFIG_ENABLED=true
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
