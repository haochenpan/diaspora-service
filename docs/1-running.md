## Running in Virtual Environment

### Action Provider

To test the Action Provider in a virtual environment, run the main script:

```bash
source secrets.sh
python action_provider/main.py
```

### Web Service

To test the Web Service in a virtual environment, use the following command to start the server:

```bash
source secrets.sh
uvicorn web_service.main:app --host 0.0.0.0 --port 8000 --reload
```

## Running in Docker Container

### Action Provider

To build and run the Action Provider in a Docker container:

1. Define environment variables:

    ```bash
    SERVICE_NAME=diaspora-action-provider
    CONTAINER_NAME=container
    DOCKERFILE_PATH=action_provider/Dockerfile
    ```

2. Build the Docker image:

    ```bash
    docker build -t $CONTAINER_NAME -f $DOCKERFILE_PATH .
    ```

3. Run the container:

    ```bash
    source secrets.sh
    docker run -p 8000:8000 \
    -e AWS_ACCESS_KEY_ID \
    -e AWS_SECRET_ACCESS_KEY \
    -e CLIENT_ID \
    -e CLIENT_SECRET \
    -e CLIENT_SCOPE \
    -e DEFAULT_SERVERS \
    $CONTAINER_NAME
    ```

### Web Service

To build and run the Web Service in a Docker container:

1. Define environment variables:

    ```bash
    SERVICE_NAME=diaspora-web-service
    CONTAINER_NAME=container
    DOCKERFILE_PATH=web_service/Dockerfile
    ```

2. Build the Docker image:

    ```bash
    docker build -t $CONTAINER_NAME -f $DOCKERFILE_PATH .
    ```

3. Run the container:

    ```bash
    source secrets.sh
    docker run -p 8000:8000 \
    -e AWS_ACCESS_KEY_ID \
    -e AWS_SECRET_ACCESS_KEY \
    -e SERVER_CLIENT_ID \
    -e SERVER_SECRET \
    $CONTAINER_NAME
    ```
