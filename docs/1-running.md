## Running in Virtual Environment

### Web Service

To run the Web Service in a virtual environment, follow these steps:

1. Source your secrets:

   ```bash
   source secrets2.sh  # pragma: allowlist secret
   ```

2. Start the server in development mode using Uvicorn:

   ```bash
   uvicorn web_service.main:app --host 0.0.0.0 --port 8000 --reload
   ```

3. For production mode, use Gunicorn with Uvicorn workers to start the server:

   ```bash
   gunicorn --bind 0.0.0.0:8000 -k uvicorn.workers.UvicornWorker web_service.main:app
   ```


## Running in Docker Container

### Web Service

To build and run the Web Service in a Docker container:

1. Define environment variables:

    ```bash
    SERVICE_NAME=diaspora-web-service
    CONTAINER_NAME=docker
    DOCKERFILE_PATH=web_service/Dockerfile  # pragma: allowlist secret
    ```

2. Build the Docker image:

    ```bash
    docker build -t $CONTAINER_NAME -f $DOCKERFILE_PATH .
    ```

3. Run the container:

    ```bash
    source secrets2.sh  # pragma: allowlist secret
    docker run -p 8000:8000 \
    -e AWS_ACCESS_KEY_ID \
    -e AWS_SECRET_ACCESS_KEY \
    -e SERVER_CLIENT_ID \
    -e SERVER_SECRET \
    -e AWS_ACCOUNT_ID \
    -e AWS_ACCOUNT_REGION \
    -e MSK_CLUSTER_NAME \
    $CONTAINER_NAME
    ```
