import os
import logging
import transformation as transform
import boto3
import pandas as pd
from dynamodb_json import json_util as json
import numpy as np

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.info(event)
logger.info(context)

# Initialize boto3 clients
sns = boto3.client('sns')

def notify(text):
    try:
        sns.publish(TopicArn=os.environ['sns_topic'],
                    Subject='Covid19 ETL summary',
                    Message=text)
    except Exception as e:
        logger.error("Sending notification failed: {}".format(e))
        exit(1)
    logger.info(text)

def lambda_handler(event, context):

    # Extraction
    df_nyt = pd.read_csv(nyt_url, index_col='date')
    df_jh = pd.read_csv(jh_url, index_col='Date')

    # Transformation
    df_nyt = transform.convert_to_date_obj(df_nyt, 'date', '%Y-%m-%d')
    df_nyt = transform.convert_to_int_obj(df_nyt, 'cases')
    df_nyt = transform.convert_to_int_obj(df_nyt, 'deaths')

    df_jh = transform.convert_to_date_obj(df_jh, 'Date', '%Y-%m-%d')
    df_jh = transform.convert_to_int_obj(df_nyt, 'Recovered')
    df_jh = transform.filter_rows(df_jh, 'Country/Region', 'US')
    df_jh = transform.filter_columns(df_jh, ['Date', 'Recovered'])
    df_jh = df_jh.rename(columns={'Date': 'date', 'Recovered':'recoveries'})

    df_joined = transform.merge(df_nyt, df_jh[['date', 'recoveries']], 'date', 'left')
    df_joined = transform.drop_nonexistent(df_joined)
