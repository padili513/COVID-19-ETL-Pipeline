import os
import logging
import transformation as transform
import boto3
import pandas as pd
import numpy as np

def alert():
    """Sends the alert"""
    message = ""

    func_name = context.function_name
    logger.info(func_name)
    trigger_arn = context.invoked_function_arn
    logger.info(trigger_arn)
    log_group_name = context.log_group_name
    logger.info(log_group_name)
    log_stream_name = context.log_stream_name
    logger.info(log_stream_name)

    message += (
        f"Function: {func_name}\n"
        f"Trigger: {trigger_arn}\n"
        f"Log Group: {log_group_name}\n"
        f"Log Stream: {log_stream_name}\n"
        f"\nNew rows added:\n"
    )

    new_rows = []
    for record in event["Records"]:
        if record["eventName"] == "INSERT":
            if "NewImage" in record["dynamodb"]:
                new_rows.append(record["dynamodb"]["NewImage"])

    new_rows_str = [str(x) for x in new_rows]
    message += "\n".join(new_rows_str)

    sns.publish(TopicArn=topic_arn, Message=message)
    return message

def db_connect():
    """Connects to the DynamoDB Table"""
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    return dynamodb.Table("covid-19-table")

def db_store(df: pd.DataFrame):
    """Stores Pandas DataFrame into DynamoDB"""
    json_data = df.T.to_dict().values()
    for entry in json_data:
        try:
            entry["recoveries"] = int(entry["recoveries"])
        except ValueError:
            entry["recoveries"] = 0
        logger.info("Storing: %s", entry)
        db_table.put_item(Item=entry)

def db_load() -> pd.DataFrame:
    """Loads data from DynamoDB into Pandas DataFrame"""
    print(db_table)
    response = db_table.scan()
    data = response["Items"]
    while "LastEvaluatedKey" in response:
        response = db_table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        data.extend(response["Items"])
    return pd.DataFrame(json.loads(data))

def find_new(cur_data: pd.DataFrame, new_data: pd.DataFrame) -> pd.DataFrame:
    """Takes in two Pandas DataFrames, finds which rows exist only in the
    'right' dataframe and returns those rows only as a new DataFrame"""
    return new_data[np.equal(new_data.date.isin(cur_data.date), False)]

def data_diff(df: pd.DataFrame) -> pd.DataFrame:
    """Compares a DataFrame with data already stored in the DB"""
    old_df = db_load()
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

    sns = boto3.client('sns')
    topic_arn = sns.list_topics()['Topics'][0]['TopicArn']
    # table_name = 'covid-19-table'
    dynamodb = boto3.client('dynamodb')
    waiter = dynamodb.get_waiter('table_exists')
    waiter.wait(TableName="covid-19-table")
    dynamodb = boto3.resource('dynamodb')
    db_table = dynamodb.Table("covid-19-table")
    print(db_table.table_arn)

    try:
        # Extraction
        df_nyt = pd.read_csv(os.environ['nyt_url'], index_col=None)
        df_jh = pd.read_csv(os.environ['jh_url'], index_col=None)

        # Transformation
        df_nyt = transform.convert_to_date_obj(df_nyt, 'date', '%Y-%m-%d')

        df_jh = transform.convert_to_date_obj(df_jh, 'Date', '%Y-%m-%d')
        df_jh = transform.filter_rows(df_jh, 'Country/Region', 'US')
        df_jh = transform.filter_columns(df_jh, ['Date', 'Recovered'])
        df_jh = df_jh.rename(columns={'Date': 'date', 'Recovered':'recoveries'})

        df_joined = transform.merge(df_nyt, df_jh[['date', 'recoveries']], 'date', 'left')
        df_joined = transform.drop_nonexistent(df_joined)
        transform.convert_to_int_obj(df_joined, 'recoveries')

        # Load
        new_data = data_diff(df_joined)
        logger.info("New data: %s", new_data)
        db_store(new_data)
        alert()
    except Exception as e:
        # Doing a general catch because I want all errors to be pushed to SNS
        sns.publish(TopicArn=topic_arn, Message=(
            f"There was an error in function {context.function_name}.\n"
            f"Please see log {context.log_group_name} for more info.\n"
            f"Error: {e}"
        ))
        exit(1)