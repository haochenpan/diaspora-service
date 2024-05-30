# Deployment
## Deploy through GitHub Actions

A push or merge to the `main` branch automatically triggers AWS Lightsail Docker deployments. See the workflow file [lightsail.yml](https://github.com/haochenpan/diaspora-service/blob/main/.github/workflows/lightsail.yml). This action is partially adapted from [fdiesel/github-action-deploy-aws-lightsail-container](https://github.com/fdiesel/github-action-deploy-aws-lightsail-container).

## Deploy from a Local Environment

To deploy from a local environment, first build the Docker containers using the commands provided above. Then, use the following commands to push the images to AWS Lightsail and deploy them.

1. Delete old container images:

    ```bash
    output=$(aws lightsail get-container-images --service-name $SERVICE_NAME --no-paginate --output text)
    container_names=($(echo "$output" | awk '{print $NF}'))
    for name in "${container_names[@]:1}"; do
        echo "IMAGE TO DELETE: $name"
        aws lightsail delete-container-image --region us-east-1 --service-name $SERVICE_NAME --image "$name" || true
    done
    ```

2. Idempotently create Lightsail container service:

    ```bash
    aws lightsail create-container-service --region us-east-1 --service-name $SERVICE_NAME --power small --scale 1 || true
    ```

3. Push Docker image to Lightsail and deploy:

    ```bash
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
    ```
