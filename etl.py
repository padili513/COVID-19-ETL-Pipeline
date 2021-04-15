import os
import logging
import transformation as transform
import boto3
import pandas as pd
from dynamodb_json import json_util as json
import numpy as np

def lambda_handler(event, context):

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.info(event)
    logger.info(context)

    # Initialize boto3 clients
    sns = boto3.client('sns')