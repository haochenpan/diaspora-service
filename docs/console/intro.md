The script below is the startup script for the Diaspora Admin Console on Lightsail instance at [100.27.155.7](http://100.27.155.7) with credentials masked.

### Script Overview

This script sets up a Docker environment, installs Docker Compose, and configures a Kafka UI service with necessary environment variables and security configurations. It also sets up a systemd service to manage the Docker Compose application and redirects HTTP traffic from port 80 to port 8080. Finally, it schedules a reboot every three hours using cron.

### Masked Credentials

- `SPRING_SECURITY_USER_NAME`: `********`
- `SPRING_SECURITY_USER_PASSWORD`: `********`
- `AWS_ACCESS_KEY_ID`: `********************`
- `AWS_SECRET_ACCESS_KEY`: `****************************`


### Startup Script

See [startup_script.sh](https://github.com/haochenpan/diaspora-service/blob/main/admin_console/startup_script.sh)