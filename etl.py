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

    