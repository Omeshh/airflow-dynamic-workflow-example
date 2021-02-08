import boto3
import botocore
import datetime
import json
import shlex

from datetime import datetime
from airflow.hooks.base_hook import BaseHook

TMP_CONFIG_FILE_LOCATION = '/tmp/config-dev.json'


def create_spark_submit(spark_args, conf_conn_id):
    """
    Create the spark submit array required for submitting to emr
    :param spark_args: Dict
    :param conf_conn_id: String The placeholder for password from argument spark_jvm_options
                                will get replaced with the actual password for the passed Airflow connection id
    :return: List of Strings
    """
    command = "spark-submit"

    # Spark opts build for spark-submit
    spark_opts = spark_args['opts']
    for key, value in spark_opts.items():
        command = command + ' --' + key + ' ' + value

    # Spark conf build for spark-submit
    spark_conf = spark_args["conf"]
    for key, value in spark_conf.items():
        # Special handling of aurora password
        if conf_conn_id is not None and 'extraJavaOptions' in key:
            connection = BaseHook.get_connection(conf_conn_id)
            password = connection.password
            if password:
                value = value.replace('<password>', password)

        command = command + ' --conf "' + key + '=' + value + '"'

    # Spark jar build for spark-submit
    spark_jar = spark_args["jar"]
    command = command + ' ' + spark_jar["location"]
    for key, value in spark_jar['args'].items():
        command = command + ' --{}={}'.format(key, value)

    return shlex.split(command)


def create_emr_conf(emr_conf):
    res = []
    for key, value in emr_conf.items():
        res_entry = {'Classification': key, 'Properties': {}}
        if key == "spark-env":
            env_conf = []
            env_conf_entry = {'Classification': 'export', 'Properties': value}
            env_conf.append(env_conf_entry)
            res_entry['Configurations'] = env_conf
        else:
            res_entry.update({'Properties': value})

        res.append(res_entry)

    return res


def create_aws_tags(aws_args):
    tags = []
    for key, value in aws_args["tags"].items():
        entry = {
            "Key": key,
            "Value": value
        }
        tags.append(entry)

    return tags


def create_job_flow_overrides(
        emr_job_type_tag,
        emr_name,
        emr_log_uri,
        key_name,
        subnet_id,
        security_group,
        team_tag='db',
        emr_version='5.20.0',
        master_instance_type='m4.xlarge',
        slave_instance_type='m4.xlarge',
        slave_instance_count=2,
        bootstrap_actions=None,
        cost_code=None):
    """
    Create the spark job flow overrides array required for creating emr cluster
    :param emr_job_type_tag: String tag name for emr job
    :param emr_name: String name of emr cluster
    :param emr_log_uri: String emr log location for storing emr logs
    :param key_name: String ec2 key name
    :param subnet_id: String ec2 subnet id
    :param security_group: String security group id
    :param team_tag: String tag for team name
    :param emr_version: String version of emr
    :param master_instance_type: String ec2 instance type for master
    :param slave_instance_type: String ec2 instance type for slave
    :param slave_instance_count: Integer Number of instances for slaves
    :param bootstrap_actions: semi-colin separated list of bootstrap scripts to execute
    :return: List json
    """
    # Set the directory name of the calling module as default
    if bootstrap_actions is None:
        bootstrap_list = []
    else:
        bootstrap_list = list(
            {"Name": x.split(" ")[0], "ScriptBootstrapAction": {"Path": x.split(" ")[0], "Args": x.split(" ")[1:]}}
            for x in bootstrap_actions.split(";"))

    aws_tags = {
        'tags': {
            'team': team_tag,
            'emr_job_type': emr_job_type_tag
        }
    }
    if cost_code:
        aws_tags['tags']['costcode'] = cost_code

    emr_conf = {
        'spark-env': {
            'PYSPARK_PYTHON': '/usr/bin/python3'
        },
        'spark-defaults': {
            'spark.executor.memoryOverhead': '1000',
            'spark.driver.maxResultSize': '2g'

        },
        'yarn-site': {
            'yarn.resourcemanager.am.max-attempts': '1'
        },
        'spark-hive-site': {
            'hive.metastore.client.factory.class': 'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory'
        }
    }

    job_flow_overrides = {
        'Name': emr_name + '-' + datetime.utcnow().strftime('%Y/%m/%d-%R'),
        'LogUri': emr_log_uri,
        'ReleaseLabel': 'emr-' + emr_version,
        'Applications': [
            {
                'Name': 'Hadoop'
            },
            {
                'Name': 'Spark'
            }
        ],
        'Instances': {
            'InstanceGroups': [
                {
                    'Name': 'Master nodes',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': master_instance_type,
                    'InstanceCount': 1
                },
                {
                    'Name': 'Slave nodes',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': slave_instance_type,
                    'InstanceCount': slave_instance_count
                }
            ],
            'Ec2KeyName': key_name,
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False,
            'Ec2SubnetId': subnet_id,
            'AdditionalMasterSecurityGroups': [
                security_group
            ],
            'AdditionalSlaveSecurityGroups': [
                security_group
            ]
        },
        'Configurations': create_emr_conf(emr_conf),
        'VisibleToAllUsers': True,
        'JobFlowRole': 'EMR_EC2_DefaultRole',
        'ServiceRole': 'EMR_DefaultRole',
        'Tags': create_aws_tags(aws_tags),
        'BootstrapActions': bootstrap_list
    }

    return job_flow_overrides


def create_spark_job_steps(
        action_on_failure,
        spark_job_name,
        spark_jar_location,
        spark_class,
        spark_job_env,
        spark_executor_memory,
        spark_executor_core,
        spark_deploy_mode='cluster',
        spark_master='yarn',
        spark_jvm_options='',
        spark_job_args=None,
        conf_conn_id=None):
    """
    Create the spark job steps array required for submitting emr job to cluster
    :param action_on_failure: String Terminate cluster action on failure of job step
    :param spark_job_name: String Name of the spark job
    :param spark_jar_location: String Location of the spark job jar file
    :param spark_class: String Name of the main spark class
    :param spark_job_env: String Environment name for spark job
    :param spark_executor_memory: String Executor memory settings for spark job
    :param spark_executor_core: Integer Executor cores settings for spark job
    :param spark_deploy_mode: String Deploy mode for spark application
    :param spark_master: String Setting for spark master
    :param spark_jvm_options: String JVM options for spark job
    :param spark_job_args: Dict Arguments for spark job
    :param conf_conn_id: String The placeholder for password from argument spark_jvm_options
                                will get replaced with the actual password for the passed Airflow connection id
    :return: List json
    """

    spark_jvm_opts = "-Denv={}{}".format(spark_job_env, spark_jvm_options)

    spark_args = {
        'opts': {
            'deploy-mode': spark_deploy_mode,
            'master': spark_master
        },
        'conf': {
            'spark.driver.extraJavaOptions': spark_jvm_opts,
            'spark.executor.extraJavaOptions': spark_jvm_opts
        },
        'jar': {
            'location': spark_jar_location,
            'args': spark_job_args or {}
        }
    }
    if spark_executor_core:
        spark_args['opts']['executor-cores'] = spark_executor_core
    if spark_executor_memory:
        spark_args['opts']['executor-memory'] = spark_executor_memory
    if spark_class:
        spark_args['opts']['class'] = spark_class

    spark_job_steps = [
        {
            'Name': spark_job_name,
            'ActionOnFailure': action_on_failure,
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': create_spark_submit(spark_args, conf_conn_id=conf_conn_id)
            }
        }
    ]

    return spark_job_steps


def read_config(config_path,
                region,
                aws_access_key_id,
                aws_secret_access_key_id,
                tmp_config_file_loc=TMP_CONFIG_FILE_LOCATION):
    """
    Reads config file from s3 (if path starts with s3://) or read from local file system
    :param config_path: String config path can be s3://abc/def or /abc/def
    :param region: String aws region
    :param aws_access_key_id: String aws access key
    :param aws_secret_access_key_id: String aws secret key id
    :param tmp_config_file_loc: String tmp file location to download file from s3 to
    :return: Dict with json config loaded
    """
    # Check whether config path starts with s3; if not then just read from the local config path
    if config_path.startswith("s3://"):
        config_path = config_path[5:]
        download_config_from_s3(config_path,
                                region,
                                aws_access_key_id,
                                aws_secret_access_key_id,
                                tmp_config_file_loc)
        config_file_path = tmp_config_file_loc
    else:
        config_file_path = config_path

    return read_config_from_file(config_file_path)


def read_config_from_file(config_file_path):
    with open(config_file_path) as f:
        config = json.load(f)
        return config


def download_config_from_s3(config_path,
                            region,
                            aws_access_key_id,
                            aws_secret_access_key_id,
                            tmp_config_file_loc):
    idx = config_path.find('/')
    bucket_name = config_path[:idx]
    s3_key = config_path[idx + 1:]

    s3_client = boto3.client(
        's3',
        region_name=region,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key_id,
    )

    try:
        with open(tmp_config_file_loc, 'wb') as data:
            s3_client.download_fileobj(bucket_name, s3_key, data)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise

    with open(tmp_config_file_loc) as f:
        config = json.load(f)
        return config


def scale_aurora(aurora_cluster_name,
                 aws_access_key_id,
                 aws_secret_access_key_id,
                 region,
                 min_capacity=3,
                 max_capacity=3,
                 duration_minutes=60
                 ):
    """
    This function will force RDS to create read replicas and then scale back down
    after 25 minutes.
    :param region: AWS region
    :param aws_secret_access_key_id: AWS Access key
    :param aws_access_key_id: AWS Secret key
    :param aurora_cluster_name: The name of the RDS Aurora cluster to scale
    :param min_capacity: The fewest number of replicas desired. Default: 3
    :param max_capacity: The most number of replicas desired. Default: 3
    :type aurora_cluster_name: str
    :type min_capacity: int
    :type max_capacity: int
    """

    now = datetime.datetime.utcnow()
    schedule = (now + datetime.timedelta(minutes=duration_minutes)).strftime("%Y-%m-%dT%H:%M:%S")

    client = boto3.client('application-autoscaling',
                          region_name=region,
                          aws_access_key_id=aws_access_key_id,
                          aws_secret_access_key=aws_secret_access_key_id,
                          )

    client.register_scalable_target(
        ServiceNamespace='rds',
        MaxCapacity=max_capacity,
        MinCapacity=min_capacity,
        ResourceId=f'cluster:{aurora_cluster_name}',
        ScalableDimension='rds:cluster:ReadReplicaCount',
    )

    client.put_scheduled_action(
        ResourceId=f'cluster:{aurora_cluster_name}',
        ScalableDimension='rds:cluster:ReadReplicaCount',
        Schedule=f'at({schedule})',
        ScheduledActionName='emr_scale_down',
        ServiceNamespace='rds',
        ScalableTargetAction={
            'MaxCapacity': 5,
            'MinCapacity': 1
        }
    )
