# Deployment

## Local Testing in Virtual Environment
TODO

## Local Testing in Docker Container

```bash
SERVICE_NAME=diaspora-web-service
CONTAINER_NAME=container
DOCKERFILE_PATH=web_service/Dockerfile

SERVICE_NAME=diaspora-action-provider
CONTAINER_NAME=container
DOCKERFILE_PATH=action_provider/Dockerfile

echo "SERVICE NAME: $SERVICE_NAME"
echo "CONTAINER NAME: $CONTAINER_NAME"
echo "DOCKERFILE PATH: $DOCKERFILE_PATH"

docker build -t $CONTAINER_NAME -f $DOCKERFILE_PATH .
docker run -p 8000:8000 $CONTAINER_NAME
docker images
```

## Automatic Deployment through GitHub Action
A push or a merge to the `main` branch automatically triggers AWS Lightsail Docker deployments. See workflow [lightsail.yml](../.github/workflows/lightsail.yml) The following commands are equivalent to those executed by the GitHub Actions workflow:

## Deploy to AWS Lightsail from a Local Environment
```bash

# Delete old container images
output=$(aws lightsail get-container-images --service-name $SERVICE_NAME --no-paginate --output text)
container_names=($(echo "$output" | awk '{print $NF}'))
for name in "${container_names[@]:1}"; do
    echo "IMAGE TO DELETE: $name"
    aws lightsail delete-container-image --region us-east-1 --service-name $SERVICE_NAME --image "$name" || true
done

# Idempotently Create Lightsail container service
aws lightsail create-container-service --region us-east-1 --service-name $SERVICE_NAME --power small --scale 1 || true

# Push Docker image to Lightsail and deploy
output=$(aws lightsail push-container-image --region us-east-1 --service-name $SERVICE_NAME --label $CONTAINER_NAME --image $CONTAINER_NAME)
image_name=$(echo "$output" | sed -n 's/.*Refer to this image as "\(.*\)" in deployments.*/\1/p')

echo "IMAGE NAME: $image_name"

containers=$(jq -n --arg image_name "$image_name" '{
    "flask": {
        "image": $image_name,
        "ports": {
            "8000": "HTTP"
        }
    }
}')

public_endpoint=$(jq -n '{
    "containerName": "flask",
    "containerPort": 8000
}')

aws lightsail create-container-service-deployment --region us-east-1 \
    --service-name $SERVICE_NAME \
    --containers "$containers" \
    --public-endpoint "$public_endpoint"
done
```