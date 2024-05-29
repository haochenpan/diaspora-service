# diaspora-service

docker build -t diaspora-app -f lightsail/Dockerfile .

docker run -p 8000:8000 diaspora-app 

aws lightsail create-container-service --service-name diaspora-app --power micro --scale 1

aws lightsail push-container-image --region us-east-1 --service-name diaspora-app --label diaspora-app --image diaspora-app
# Refer to this image as ":diaspora-app.diaspora-app.22" in deployments.

aws lightsail create-container-service-deployment --region us-east-1 --cli-input-json



aws lightsail create-container-service --generate-cli-skeleton



docker build -t diaspora-app2 -f Dockerfile . 
docker run -p 8000:8000 diaspora-app2 

aws lightsail create-container-service --region us-east-1 --service-name diaspora-app2 --power small --scale 1
aws lightsail push-container-image --region us-east-1 --service-name diaspora-app2 --label diaspora-app2 --image diaspora-app2 --no-paginate --output json

aws lightsail create-container-service-deployment --region us-east-1 \
    --service-name diaspora-app2 \
    --containers '{
        "flask": {
            "image": ":diaspora-app2.diaspora-app2.26",
            "ports": {
                "8000": "HTTP"
            }
        }
    }' \
    --public-endpoint '{
        "containerName": "flask",
        "containerPort": 8000
    }'

```bash
# current_container="$service_name-$(openssl rand -base64 12 | tr 'A-Z' 'a-z' | tr -dc 'a-z0-9')"


service_name="diaspora-service"
current_container="$service_name-container"
echo $current_container
docker build -t $current_container -f Dockerfile . 
docker run -p 8000:8000 $current_container

aws lightsail create-container-service --region us-east-1 --service-name $service_name --power small --scale 1
output=$(aws lightsail push-container-image --region us-east-1 --service-name $service_name --label $current_container --image $current_container)
image_name=$(echo "$output" | sed -n 's/.*Refer to this image as "\(.*\)" in deployments.*/\1/p')
echo "$image_name"

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
    --service-name $service_name \
    --containers "$containers" \
    --public-endpoint "$public_endpoint"
    




service_name="diaspora-web-service"
current_container="$service_name-container"
docker build -t $current_container -f web_service/Dockerfile . 
docker run -p 8000:8000 $current_container


service_name="diaspora-action-provider"
current_container="$service_name-container"
docker build -t $current_container -f action_provider/Dockerfile . 
docker run -p 8000:8000 $current_container
```

