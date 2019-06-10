Twitter Sentiment Streamer 
==============================

What Is This?
-------------

Twitter Sentiment Streamer will stream tweets in from Twitter based off of a keyword or phrase. Once the data is streamed into AWS, it will run sentiment analysis on each tweet using Amazon Comprehend. From there, you can query that data via Athena!

Requirements
------------
- AWS Secret Key and Access Key ID that has the proper access to deploy this environment
- Twitter developer credentials to programatically pull data in from Twitter


How to use this
---------------

1. To deploy the environment, you need to set an environment variable for STACK_NAME. 
    - Example: `export STACK_NAME=testing-twitter-stream`
2. You will need to create [Twitter developer credentials](https://developer.twitter.com/content/developer-twitter/en.html).
3. Store these credentials in [Secrets Manager](https://aws.amazon.com/secrets-manager/) and set an environment variable TWITTER_SECRET_ARN with the arn. There should be four key/value pairs that are under the one secrets manager credential.
    - Example: `export TWITTER_SECRET_ARN=twitter-secrets-manager-arn-goes-here`
4. Set an environment variable `TWITTER_KEYWORD` based on what keyword(s) you want to run analysis trends on from twitter.
    - Example: `export TWITTER_KEYWORD=maga`
5. Check `env_vars_example.sh` for all environment variables used.
6. Run `./deploy.sh build`. This will build the docker container and create/push to an ECR repository. This is required.
7. Run `cdk synth`. This will give you the CloudFormation templates for the stacks to the `cdk.out` directory. Feel free to review.
8. Deploy the environment by running: `cdk deploy` or `./deploy.sh deploy`. You will be prompted to approve the deploy for each stack.
9. That's it! Assuming you have set the proper secrets arn and the proper secrets in secrets manager, data will start streaming in and will have it's own catalog to query via [Athena](https://aws.amazon.com/athena/)
10. To visualize the data, you can use [Quicksight](https://aws.amazon.com/quicksight/). Below is an example:

![alt text](https://twitter-stream-image.s3-us-west-2.amazonaws.com/maga_sentiment.png "#MAGA sentiment")


Required environment variables
------------------------------
- STACK_NAME
- REGION (AWS REGION)
- TWITTER_SECRET_ARN
- TWITTER_KEYWORD

Versions
--------

1. npm: aws-cdk@0.33.0
2. Python: aws-cdk-0.33.0


Architecture
-----------

![ArchDiagram](https://twitter-stream-image.s3-us-west-2.amazonaws.com/twitter_sentiment.png)

