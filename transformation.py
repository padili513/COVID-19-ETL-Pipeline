import pandas as pd

# Cleaning
def convert_to_date_obj(df, field, current_format):
    df[field] = pd.to_datetime(df[date], format=current_format)
    return df

def convert_to_int_obj(df, field):
    df[field] = pd.to_numeric(df[field])
    return df

# Joining
def merge(df1, df2, on_field, how_type):
    return pd.merge(df1, df2, on=on_field, how=how_type)

# Filtering
def drop_nonexistent(df):
    return df.dropna()

def filter_rows(df, field, select):
    return df[df[field] == select]

def filter_columns(df, cols):
    return df[cols]