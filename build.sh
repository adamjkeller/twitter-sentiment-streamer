#!/usr/bin/env bash

source ./env_vars.sh

docker_tag=$(git rev-parse --short=7 HEAD)
repo_name="${STACK_NAME:twitter-streamer-demo}-${ENVIRONMENT}-worker"

docker_login() {
    eval $(aws ecr get-login --no-include-email --region $REGION)
}

create_or_get_repo() {
    _repo=$(aws ecr create-repository --repository-name $repo_name)
    if [[ ! $_repo ]]; then
        _repo=$(aws ecr describe-repositories| jq -r ".repositories[] | select(.repositoryName==\"${repo_name}\") | .repositoryUri")
    fi
}

build_image() {
    echo $_repo
    docker build -t "$_repo:latest" -t "$_repo:$docker_tag" .
}

push_image() {
    _repo=$1
    docker push "$_repo:$docker_tag"
    docker push "$_repo:latest"
}

# Login to ECR via aws cli
docker_login >> /dev/null 2>&1
# Gets annoyingly verbose, so if you need to debug, comment out stderr/out to /dev/null
create_or_get_repo >> /dev/null 2>&1
# Build image
build_image $_repo
# Push it!
push_image $_repo