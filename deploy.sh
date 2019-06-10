#!/usr/bin/env bash

source ./env_vars.sh

if [[ $(echo $@) == "build" ]];then
  echo "BUILDING DOCKER IMAGE"
  ./build.sh
  exit 0
fi

export ENVIRONMENT="${ENVIRONMENT:-development}"
export STACK_NAME="${STACK_NAME:-twitter-stream}"

# pass deploy, otherwise just synth
if [[ $(echo $@) == "deploy" ]];then
  cdk deploy
  exit 0
fi

# pass deploy, otherwise just synth
if [[ $(echo $@) == "synth" ]];then
  cdk synth
  exit 0
fi

cdk diff
