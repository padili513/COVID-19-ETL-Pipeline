import os
import logging
from dynamodb_json import json_util as json
import transformation as transform
import boto3
import pandas as pd
import numpy as np

def db_store(db_table, df, logger):
    """Stores Pandas DataFrame into DynamoDB"""
    json_data = df.T.to_dict().values()
    for entry in json_data:
        try:
            entry["recoveries"] = int(entry["recoveries"])
        except ValueError:
            entry["recoveries"] = 0
        logger.info("Storing: %s", entry)
        db_table.put_item(Item=entry)

def db_load(db_table):
    """Loads data from DynamoDB into Pandas DataFrame"""
    response = db_table.scan()
    data = response["Items"]
    while "LastEvaluatedKey" in response:
        response = db_table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        data.extend(response["Items"])
    return pd.DataFrame(json.loads(data))

def find_new(cur_data, new_data):
    """Takes in two Pandas DataFrames, finds which rows exist only in the
    'right' dataframe and returns those rows only as a new DataFrame"""
    return new_data[np.equal(new_data.date.isin(cur_data.date), False)]

def data_diff(db_table, df):
    """Compares a DataFrame with data already stored in the DB"""
    old_df = db_load(db_table)
    try:
        # If there is already data in DynamoDB, this diff will work
        diff_df = find_new(old_df, df)
    except AttributeError:
        # If there is no data in DynamoDB yet, an AttributeError will be raised
        # Here we create a blank DataFrame to diff against
        df_columns = ["date", "cases", "deaths", "recoveries"]
        old_df = pd.DataFrame(columns=df_columns)
        try:
            # We now try the diff again, this nested try/except block follows
            # EAFP: Easier to ask for forgiveness than permission
            diff_df = find_new(old_df, df)
        except Exception as e:
            # I don't yet know what other possible issues could arise so general catch for now
            logger.info(e)
    return diff_df

def lambda_handler(event, context):

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.info(event)
    logger.info(context)

    sns = boto3.client("sns")
    topic_arn = os.environ["SNS_TOPIC_ARN"]
    dynamodb = boto3.resource("dynamodb")
    db_table = dynamodb.Table("covid-19-table")

    try:
        # Extraction
        df_nyt = pd.read_csv(os.environ["NYT_URL"])
        df_jh = pd.read_csv(os.environ["JH_URL"])

        # Transformation
        df_jh = transform.filter_rows(df_jh, "Country/Region", "US")
        df_jh = transform.filter_columns(df_jh, ["Date", "Recovered"])
        df_jh = df_jh.rename(columns={"Date": "date", "Recovered":"recoveries"})

        df_joined = transform.merge(df_nyt, df_jh[["date", "recoveries"]], "date", "left")
        df_joined = transform.drop_nonexistent(df_joined)
        transform.convert_to_int_obj(df_joined, "recoveries")

        # Load
        new_data = data_diff(db_table, df_joined)
        logger.info("New data: %s", new_data)
        db_store(db_table, new_data, logger)
    except Exception as e:
        # Doing a general catch because I want all errors to be pushed to SNS
        sns.publish(TopicArn=topic_arn, Message=(
            f"There was an error in function {context.function_name}.\n"
            f"Please see log {context.log_group_name} for more info.\n"
            f"Error: {e}"
        ))
        exit(1)
    