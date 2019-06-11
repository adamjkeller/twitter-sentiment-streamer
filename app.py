#!/usr/bin/env python3
import os
import sh
from aws_cdk import (
    aws_ec2,
    aws_ecs,
    aws_ecr,
    aws_glue,
    aws_iam,
    aws_kinesisfirehose,
    aws_lambda,
    aws_lambda_event_sources,
    aws_logs,
    aws_s3,
    aws_secretsmanager,
    aws_sqs,
    aws_ssm,
    cdk
)


class BaseModule(cdk.Stack):

    def __init__(self, scope: cdk.Stack, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        self.vpc = aws_ec2.Vpc(
            self, "BaseVPC",
            cidr='10.0.0.0/24',
            enable_dns_support=True,
            enable_dns_hostnames=True,
        )


class StreamModule(cdk.Stack):

    def __init__(self, scope: cdk.Stack, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        self.output_bucket = aws_s3.Bucket(
            self, "BucketTwitterStreamOutput",
            bucket_name = self.stack_name,
        )

        self.bucket_url = self.output_bucket.bucket_regional_domain_name

        # Because kinesis firehose bindings are to direct CF, we have to create IAM policy/role and attach on our own
        self.iam_role = aws_iam.Role(
            self, "IAMRoleTwitterStreamKinesisFHToS3",
            role_name="KinesisFirehoseToS3-{}".format(self.stack_name),
            assumed_by=aws_iam.ServicePrincipal(service='firehose.amazonaws.com'),
        )

        # S3 bucket actions
        self.s3_iam_policy_statement = aws_iam.PolicyStatement()
        actions = ["s3:GetBucketLocation", "s3:GetObject", "s3:ListBucket", "s3:ListBucketMultipartUploads", "s3:PutObject"]
        for action in actions:
            self.s3_iam_policy_statement.add_action(action)
        self.s3_iam_policy_statement.add_resource(self.output_bucket.bucket_arn)
        self.s3_iam_policy_statement.add_resource(self.output_bucket.bucket_arn + "/*")

        # CW error log setup
        self.s3_error_logs_group = aws_logs.LogGroup(
            self, "S3ErrorLogsGroup",
            log_group_name="{}-s3-errors".format(self.stack_name)
        )

        self.s3_error_logs_stream = aws_logs.LogStream(
            self, "S3ErrorLogsStream",
            log_group=self.s3_error_logs_group,
            log_stream_name='s3Backup'
        )

        self.firehose = aws_kinesisfirehose.CfnDeliveryStream(
            self, "FirehoseTwitterStream",
            delivery_stream_name = "{}-raw".format(self.stack_name),
            delivery_stream_type = "DirectPut",
            s3_destination_configuration={
                'bucketArn': self.output_bucket.bucket_arn,
                'bufferingHints': {
                    'intervalInSeconds': 120,
                    'sizeInMBs': 10
                },
                'compressionFormat': 'UNCOMPRESSED',
                'roleArn': self.iam_role.role_arn,
                'cloudWatchLoggingOptions': {
                    'enabled': True,
                    'logGroupName': "{}-raw".format(self.stack_name),
                    'logStreamName': 's3BackupRaw'
                },
                'prefix': 'twitter-raw/'
            },
        )

        # TODO: Only attach what's needed for this policy, right now i'm lazy and attaching all policies
        self.iam_policy = aws_iam.Policy(
            self, "IAMPolicyTwitterStreamKinesisFHToS3",
            policy_name="KinesisFirehoseToS3-{}".format(self.stack_name),
            statements=[self.s3_iam_policy_statement],
        )

        self.iam_policy.attach_to_role(self.iam_role)

        # Because kinesis firehose bindings are to direct CF, we have to create IAM policy/role and attach on our own
        self.curator_firehose = aws_kinesisfirehose.CfnDeliveryStream(
            self, "CuratorFirehoseStream",
            delivery_stream_name = "{}-curator".format(self.stack_name),
            delivery_stream_type = "DirectPut",
            s3_destination_configuration={
                'bucketArn': self.output_bucket.bucket_arn,
                'bufferingHints': {
                    'intervalInSeconds': 120,
                    'sizeInMBs': 10
                },
                'compressionFormat': 'UNCOMPRESSED',
                'roleArn': self.iam_role.role_arn,
                'cloudWatchLoggingOptions': {
                    'enabled': True,
                    'logGroupName': "{}-curator".format(self.stack_name),
                    'logStreamName': 's3BackupCurator'
                },
                'prefix': 'twitter-curated/'
            },
        )

        def zip_package():
            cwd = os.getcwd()
            file_name = 'curator-lambda.zip'
            zip_file = cwd + '/' + file_name

            os.chdir('src/')
            sh.zip('-r9', zip_file, '.')
            os.chdir(cwd)

            return file_name, zip_file

        _, zip_file = zip_package()

        self.twitter_stream_curator_lambda_function = aws_lambda.Function(
            self, "TwitterStreamCuratorLambdaFunction",
            function_name="{}-curator".format(self.stack_name),
            code=aws_lambda.AssetCode(zip_file),
            handler="sentiment_analysis.lambda_handler",
            runtime=aws_lambda.Runtime.PYTHON37,
            tracing=aws_lambda.Tracing.Active,
            description="Triggers from S3 PUT event for twitter stream data and transorms it to clean json syntax with sentiment analysis attached",
            environment={
                "STACK_NAME": self.stack_name,
                "FIREHOSE_STREAM": self.curator_firehose.delivery_stream_name
            },
            memory_size=128,
            timeout=120,
            log_retention_days=aws_logs.RetentionDays.OneWeek,
        )

        # Permission to talk to comprehend for sentiment analysis
        self.comprehend_iam_policy_statement = aws_iam.PolicyStatement()
        self.comprehend_iam_policy_statement.add_action('comprehend:*')
        self.comprehend_iam_policy_statement.add_all_resources()
        self.twitter_stream_curator_lambda_function.add_to_role_policy(self.comprehend_iam_policy_statement)

        # Permission to put in kinesis firehose
        self.curator_firehose_iam_policy_statement = aws_iam.PolicyStatement()
        self.curator_firehose_iam_policy_statement.add_action('firehose:Put*')
        self.curator_firehose_iam_policy_statement.add_resource(self.curator_firehose.delivery_stream_arn)
        self.twitter_stream_curator_lambda_function.add_to_role_policy(self.curator_firehose_iam_policy_statement)

        # Attaching the policy to the IAM role for KFH
        self.output_bucket.grant_read(self.twitter_stream_curator_lambda_function)

        self.twitter_stream_curator_lambda_function.add_event_source(
            aws_lambda_event_sources.S3EventSource(
                bucket=self.output_bucket,
                events=[
                    aws_s3.EventType.ObjectCreated
                ],
                filters=[
                    aws_s3.NotificationKeyFilter(
                        prefix="twitter-raw/"
                    )
                ]
            )
        )



class TwitterStreamWorker(cdk.Stack):

    def __init__(self, scope: cdk.Stack, id: str, base_module, stream_module, **kwargs):
        super().__init__(scope, id, **kwargs)
        self.base_module = base_module
        self.stream_module = stream_module

        # Added the keys manually, this needs to be imported. Likely should be an argument we pass to stack, ie self.secrets_arn = passed-in-to-constructor
        self.twitter_secrets = aws_secretsmanager.Secret(self, "TwitterSecrets").from_secret_arn(
            self, "TwitterSecretARN",
            secret_arn=os.getenv("TWITTER_SECRET_ARN")
        )

        self.cluster = aws_ecs.Cluster(
            self, "ECSCluster",
            vpc=self.base_module.vpc,
        )

        self.image_repo = aws_ecr.Repository.from_repository_name(
            self, "ECRImport",
            repository_name=self.stack_name
        )

        #Queue to push last updated id
        self.twitter_id_queue = aws_sqs.Queue(
            self, "TwitterWorkerQueue",
            queue_name="{}.fifo".format(self.stack_name),
            fifo=True,
            content_based_deduplication=True,
            visibility_timeout_sec=90
        )

        # SSM parameter to indicate if initial run has occurred
        self.initial_run_parameter = aws_ssm.StringParameter(
            self, "StringParameterInitialRun",
            name="/{}-NOT-first-run".format(self.stack_name), # TODO: This is how naming should work with slash in front
            string_value='False',
            description="Parameter for twitter stream feed to set to true after first run has occurred and an object has been put in queue"
        )

        self.task_definition = aws_ecs.FargateTaskDefinition(
            self, "TwitterWorkerTD",
            cpu='256',
            memory_mi_b='0.5GB',
        )

        self.task_definition.add_container(
            "ContainerImage",
            image=aws_ecs.ContainerImage.from_ecr_repository(self.image_repo, tag='latest'),
            logging=aws_ecs.AwsLogDriver(self, "AWSLogsDriver", stream_prefix=self.stack_name, log_retention_days=aws_logs.RetentionDays.ThreeDays),
            environment={
                "FIREHOSE_NAME": self.stream_module.firehose.delivery_stream_name,
                "SQS_QUEUE_NAME": self.twitter_id_queue.queue_name,
                "SSM_PARAM_INITIAL_RUN": self.initial_run_parameter.parameter_name,
                "TWITTER_KEYWORD": os.getenv("TWITTER_KEYWORD") or 'maga',
                "SINCE_DATE": '2019-03-01',
                "WORLD_ID": '23424977'
            },
        )

        # IAM Permissions for fargate task
        # Adding firehose put policy
        self.firehose_iam_policy_statement = aws_iam.PolicyStatement()
        self.firehose_iam_policy_statement.add_action('firehose:Put*')
        self.firehose_iam_policy_statement.add_resource(self.stream_module.firehose.delivery_stream_arn)

        # Permission to talk to comprehend for sentiment analysis
        self.comprehend_iam_policy_statement = aws_iam.PolicyStatement()
        self.comprehend_iam_policy_statement.add_action('comprehend:*')
        self.comprehend_iam_policy_statement.add_all_resources()

        self.task_iam_policy = aws_iam.Policy(
            self, "IAMPolicyTwitterStreamFargate",
            policy_name="TwitterStreamingFargate-{}".format(self.stack_name),
            statements=[self.firehose_iam_policy_statement],
        )

        self.twitter_secrets.grant_read(self.task_definition.task_role)
        self.task_iam_policy.attach_to_role(self.task_definition.task_role)
        self.twitter_id_queue.grant_send_messages(self.task_definition.task_role)
        self.twitter_id_queue.grant_consume_messages(self.task_definition.task_role)
        self.initial_run_parameter.grant_read(self.task_definition.task_role)
        self.initial_run_parameter.grant_write(self.task_definition.task_role)

        self.fargate_service = aws_ecs.FargateService(
            self, "TwitterWorker",
            service_name=self.stack_name,
            task_definition=self.task_definition,
            cluster=self.cluster,
            maximum_percent=100,
            minimum_healthy_percent=0
        )


class TwitterDatabase(cdk.Stack):

    def __init__(self, scope: cdk.Stack, id: str, base_module, stream_module, **kwargs):
        super().__init__(scope, id, **kwargs)
        self.base_module = base_module
        self.stream_module = stream_module

        self.glue_service_iam_role = aws_iam.Role(
            self, "GlueIAMRole",
            role_name="GlueCrawler-{}".format(self.stack_name),
            assumed_by=aws_iam.ServicePrincipal(service='glue.amazonaws.com'),
        )

        # Attaching the default aws managed role, and s3 access policy to curated bucket path
        self.glue_service_iam_role.attach_managed_policy('arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole')
        self.glue_s3_iam_policy_statement = aws_iam.PolicyStatement()
        actions = ["s3:GetObject", "s3:PutObject"]
        for action in actions:
            self.glue_s3_iam_policy_statement.add_action(action)
        self.glue_s3_iam_policy_statement.add_resource(self.stream_module.output_bucket.bucket_arn + '/twitter-curated/*')

        self.glue_iam_policy = aws_iam.Policy(
            self, "GlueIAMPolicy",
            statements=[self.glue_s3_iam_policy_statement],
        )

        self.glue_iam_policy.attach_to_role(self.glue_service_iam_role)

        self.glue_database = aws_glue.Database(
            self, "GlueDatabaseTwitterData",
            database_name=self.stack_name,
        )

        self.glue_crawler = aws_glue.CfnCrawler(
            self, "GlueCrawlerTwitterDB",
            database_name = self.glue_database.database_name,
            role = self.glue_service_iam_role.role_arn,
            targets={
                "s3Targets": [
                    {
                        "path": "s3://{}/twitter-curated/".format(self.stream_module.output_bucket.bucket_name)
                    }
                ]
            },
            table_prefix=self.stack_name
        )



class MainApp(cdk.App):

    def __init__(self, _stack_name, **kwargs):
        super().__init__(**kwargs)
        self._stack_name = _stack_name

        self.base_module = BaseModule(self, _stack_name + "-base")
        self.stream_module = StreamModule(self, self._stack_name + "-stream")
        self.twitter_worker = TwitterStreamWorker(self, self._stack_name + "-worker", self.base_module, self.stream_module)
        self.database_module = TwitterDatabase(self, _stack_name + "-database", self.base_module, self.stream_module)


if __name__ == '__main__':
    _environment = os.getenv('ENVIRONMENT')
    _stack_name = os.getenv('STACK_NAME')
    app = MainApp(_stack_name=_stack_name + "-" + _environment)
    app.run()