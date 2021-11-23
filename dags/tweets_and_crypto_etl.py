import configparser
from datetime import datetime, timedelta
import boto3
import os
import configparser
import os
import json
import glob
import psycopg2
import pandas as pd
import boto3
import boto
import os
import logging 
from airflow import DAG
import time
from airflow.operators.dummy_operator import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from helpers import SqlQueries
from helpers import analysis_queries
import configparser


log_path = "s3://udacity-dend/log_json_path.json"

default_args = {
    'owner': 'mkg',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
    'start_date': datetime.now(),
}

dag = DAG('capstone_etl',
          default_args=default_args,
          description='Create Redshift Cluster, Redshift Tables, Run S3 to Redshift ETL, and Save Redshift ETL to S3, then Delete Redshift Cluster',
          schedule_interval='@yearly',
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

def create_cluster():
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')

    DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
    DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_DB                 = config.get("DWH","DWH_DB")
    DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT               = config.get("DWH","DWH_PORT")

    DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

    (DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB)

    pd.DataFrame({"Param":
                    ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
                "Value":
                    [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
                })


    ec2 = boto3.resource('ec2',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )

    s3 = boto3.resource('s3',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                    )

    iam = boto3.client('iam',aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET,
                        region_name='us-west-2'
                    )

    redshift = boto3.client('redshift',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )

    ##
    ## CREATE READ_ONLY iam role
    ##

    from botocore.exceptions import ClientError

    #1.1 Create the role, 
    try:
        print("1.1 Creating a new IAM Role") 
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                'Effect': 'Allow',
                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                'Version': '2012-10-17'})
        )    
    except Exception as e:
        print(e)
        
        
    print("1.2 Attaching Policy")

    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                        )['ResponseMetadata']['HTTPStatusCode']

    print("1.3 Get the IAM role ARN")
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

    print(roleArn)

    ##
    ## CREATE REDSHIFT CLUSTER
    ##

    try:
        response = redshift.create_cluster(        
            #HW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            #Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            
            #Roles (for s3 access)
            IamRoles=[roleArn]  
        )
    except Exception as e:
        print(e)


    # See Cluster Status

    def prettyRedshiftProps(props):
        pd.set_option('display.max_colwidth', -1)
        keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
        x = [(k, v) for k,v in props.items() if k in keysToShow]
        return pd.DataFrame(data=x, columns=["Key", "Value"])

    # print('sleeping for 180 seconds')
    # time.sleep(180)
    # print('done sleeping')

    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

    propsDict = prettyRedshiftProps(myClusterProps)
    status = propsDict[propsDict['Key'] == 'ClusterStatus'].to_dict()['Value'][2]
    logging.info('cluster status = ' + status)

    try:
        while(status != 'available'):
            myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
            propsDict = prettyRedshiftProps(myClusterProps)
            status = propsDict[propsDict['Key'] == 'ClusterStatus'].to_dict()['Value'][2]

    finally: 
        if propsDict[propsDict['Key'] == 'ClusterStatus'].to_dict()['Value'][2] == 'available':
            # After Cluster Status Changes to Available 
            # get Endpoint and Role ARN from Cluster Properties
            propsDict = prettyRedshiftProps(myClusterProps)

    DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
    DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
    print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
    print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)

    logging.info(status)


    # Open an incoming TCP port to access the cluster endpoint
    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print(e)

create_cluster = PythonOperator(
    task_id='create_cluster',
    python_callable=create_cluster,
    dag=dag
)

create_schemas = PostgresOperator(
    task_id="create_crypto_tweets_and_dim_schemas",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.create_schemas
)

create_stg_tables = PostgresOperator(
    task_id="create_staging_and_dim_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.create_stg_tables
)


stage_cg_token_list_to_redshift = StageToRedshiftOperator(
    task_id='Stage_cg_token_list',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id = "aws_credentials",
    s3_bucket = "mkgpublic",
    s3_key = "capstone/crypto/cg_token_list.csv",
    table="crypto.cg_coin_list_stg",
    extra="delimiter '|' IGNOREHEADER 1",
)


stage_snscrape_tweets_stg_to_redshift = StageToRedshiftOperator(
    task_id='Stage_snscrape_tweets_stg',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id = "aws_credentials",
    s3_bucket = "mkgpublic",
    s3_key = "capstone/tweets/tweets.parquet",
    table="tweets.snscrape_tweets_stg",
    extra="FORMAT AS PARQUET;"
)

stage_cg_hourly_to_redshift = StageToRedshiftOperator(
    task_id='Stage_cg_hourly_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id = "aws_credentials",
    s3_bucket = "mkgpublic",
    s3_key = "capstone/crypto/cg_hourly.csv",
    table="crypto.cg_hourly_stg",
    extra="delimiter '|' IGNOREHEADER 1",
)

load_cg_coin_list_table = LoadDimensionOperator(
    task_id='Load_cg_coin_list_table',
    dag=dag,
    redshift_conn_id="redshift",
    query=SqlQueries.cg_coin_list_insert,
    table = "crypto.cg_coin_list",
    mode="delete"
)

create_fact_tables = PostgresOperator(
    task_id="create_fact_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.create_fact_tables
)

load_date_dim_tweets_table = LoadDimensionOperator(
    task_id='Load_date_dim_tweets_table',
    dag=dag,
    redshift_conn_id="redshift",
    query=SqlQueries.date_dim_tweets_insert,
    table = "dim.date_dim",
    mode="delete"
)

load_date_dim_crypto_table = LoadDimensionOperator(
    task_id='Load_date_dim_crypto_table',
    dag=dag,
    redshift_conn_id="redshift",
    query=SqlQueries.date_dim_crypto_insert,
    table = "dim.date_dim",
    mode="append"
)

load_coin_stats_hist_table = LoadFactOperator(
    task_id='Load_coin_stats_hist_table',
    dag=dag,
    redshift_conn_id="redshift",
    query=SqlQueries.coin_stats_hist_insert,
    table = "crypto.coin_stats_hist",
    mode="delete"
)

load_snscrape_tweets_hist_table = LoadFactOperator(
    task_id='Load_snscrape_tweets_hist_table',
    dag=dag,
    redshift_conn_id="redshift",
    query=SqlQueries.snscrape_tweets_hist_insert,
    table = "tweets.snscrape_tweets_hist",
    mode="delete"
)

def redshift_to_s3_load():
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')

    DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
    DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

    DWH_HOST               = config.get("DWH","HOST")

    S3_BUCKET                 = config.get("S3","BUCKET")

    S3_URI                = config.get("S3","URI")

    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_DB                 = config.get("DWH","DWH_DB")
    DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT               = config.get("DWH","DWH_PORT")

    conn = psycopg2.connect(f"host={DWH_HOST} dbname={DWH_DB} user={DWH_DB_USER} password={DWH_DB_PASSWORD} port={DWH_PORT}")

    snscrape_tweets_hist_sql = """
    SELECT * FROM tweets.snscrape_tweets_hist;
    """

    coin_stats_hist_sql = """
    SELECT * FROM crypto.coin_stats_hist;
    """

    date_dim_sql = """
    SELECT * FROM dim.date_dim;
    """

    cg_coin_list_sql = """
    SELECT * FROM crypto.cg_coin_list;
    """

    min_and_max_price_usd_sql = """
        SELECT * FROM crypto.usd_price_per_month_stats
    """

    tables = {"snscrape_tweets_hist":snscrape_tweets_hist_sql,
    "coin_stats_hist":coin_stats_hist_sql,
    "date_dim":date_dim_sql,
    "cg_coin_list":cg_coin_list_sql,
    "cg_usd_min_anx_max":min_and_max_price_usd_sql,
    }

    AWS_S3_BUCKET = S3_BUCKET
    # AWS_S3_URI = S3_URI
    AWS_ACCESS_KEY_ID = KEY
    AWS_SECRET_ACCESS_KEY = SECRET

    # users_df = pd.read_sql_query(users_sql, cur)
    # songs_df = pd.read_sql_query(songs_sql, cur)
    # artists_df = pd.read_sql_query(artists_sql, cur)
    # songplays_df = pd.read_sql_query(songplays_sql, cur)
    # time_df = pd.read_sql_query(time_sql, cur)

    key = 'capstone/end_result/'

    for table in tables.keys():
        print("running: \n " + tables[table])

        df = pd.read_sql_query(tables[table], con=conn)

        print("starting write: \n " + tables[table] + " \n to S3")
        df.to_csv(
            f"s3://{AWS_S3_BUCKET}/{key}{table}.csv",
            index=False,
            storage_options={
                "key": AWS_ACCESS_KEY_ID,
                "secret": AWS_SECRET_ACCESS_KEY,
                # "token": AWS_SESSION_TOKEN,
            },
        )
        print("finished writing: \n " + tables[table] + " \n to S3")
    

    conn.close()

redshift_to_s3 = PythonOperator(
    task_id='redshift_to_s3',
    python_callable=redshift_to_s3_load,
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    queries = [analysis_queries.cg_coin_list_check,
    analysis_queries.coin_stats_hist_check,
    analysis_queries.snscrape_tweets_hist_check,
    analysis_queries.max_and_min_price_per_month_usd]
)

##
###  Delete Cluster
##
def delete_cluster():
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')

    DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
    DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_DB                 = config.get("DWH","DWH_DB")
    DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT               = config.get("DWH","DWH_PORT")

    DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

    (DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB)

    pd.DataFrame({"Param":
                    ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
                "Value":
                    [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
                })


    ec2 = boto3.resource('ec2',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )

    s3 = boto3.resource('s3',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                    )

    iam = boto3.client('iam',aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET,
                        region_name='us-west-2'
                    )

    redshift = boto3.client('redshift',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )

    #### CAREFUL!!
    print("Deleting Cluster")
    #-- Uncomment & run to delete the created resources
    try:
        redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
    except Exception as e:
        print(e)
    #### CAREFUL!!

    # See Cluster Status

    def prettyRedshiftProps(props):
        pd.set_option('display.max_colwidth', -1)
        keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
        x = [(k, v) for k,v in props.items() if k in keysToShow]
        return pd.DataFrame(data=x, columns=["Key", "Value"])

    # print('sleeping for 180 seconds')
    # time.sleep(180)
    # print('done sleeping')

    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

    propsDict = prettyRedshiftProps(myClusterProps)
    status = propsDict[propsDict['Key'] == 'ClusterStatus'].to_dict()['Value'][2]
    logging.info('cluster status = ' + status)

    try:
        while(status != 'available' or status != 'deleting'):
            myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
            propsDict = prettyRedshiftProps(myClusterProps)
            status = propsDict[propsDict['Key'] == 'ClusterStatus'].to_dict()['Value'][2]
    except Exception as e:
        if(e) == 'ClusterNotFoundFault':
            print("cluster deleted")
            return

    finally: 
        if propsDict[propsDict['Key'] == 'ClusterStatus'].to_dict()['Value'][2] != 'deleting':
            # After Cluster Status Changes to Available 
            # get Endpoint and Role ARN from Cluster Properties
            propsDict = prettyRedshiftProps(myClusterProps)
            print('cluster deleted')
        

delete_cluster = PythonOperator(
    task_id='delete_cluster',
    python_callable=delete_cluster,
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> create_cluster
create_cluster >> create_schemas
create_schemas >> create_stg_tables
create_stg_tables >> stage_cg_token_list_to_redshift
create_stg_tables >> stage_snscrape_tweets_stg_to_redshift
create_stg_tables >> stage_cg_hourly_to_redshift
stage_cg_token_list_to_redshift >> load_cg_coin_list_table
stage_snscrape_tweets_stg_to_redshift >> load_date_dim_tweets_table
stage_cg_hourly_to_redshift >> load_date_dim_crypto_table
load_date_dim_crypto_table >> create_fact_tables
load_date_dim_tweets_table >> create_fact_tables
create_fact_tables >> load_coin_stats_hist_table
create_fact_tables >> load_snscrape_tweets_hist_table
load_snscrape_tweets_hist_table >> run_quality_checks
load_coin_stats_hist_table >> run_quality_checks
run_quality_checks >> redshift_to_s3 
redshift_to_s3 >> delete_cluster
delete_cluster >> end_operator


