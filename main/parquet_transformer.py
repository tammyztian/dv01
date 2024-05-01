import pandas as pd
import dask.dataframe as dd
from typing import Optional
from pydantic import BaseModel
from datetime import datetime


DEFAULT_UPLOAD_PATH =  f'../data/cleaned/{datetime.now()}_'
    # would prob load it into an s3/other file store for QA-ing purposes. 

#  Parquet Transformer:
# Language: Scala / Python
# Functionality: Transforms CSV files into Parquet tables, aligning with a developer-supplied file schema.
# Features:
# Field name, data type, and nullability alignment with the developer-supplied file schema.
# Handling of extra fields in the developer-supplied file schema.
# Consideration for missing fields in the developer-supplied file schema.
# Standardization of date fields to the ISO standard format (yyyy-MM-dd).
# Hints:
# some columns or some rows in a column are already in the ISO standard format.  Please consider performance!!!
# You may want to also consider letting the user of the library choose which column to apply standardization.

def transform_df(df:pd.DataFrame, schema_file: str):
    pass


def standardize_ISO_date_columns(df: pd.DataFrame, columns=list[str]):
    for column in columns:
        df[column] = df[column].apply(standardize_to_ISO_date)
    print(df)

    return df


def standardize_to_ISO_date(date_str):
    # import pdb; pdb.set_trace()
    date_formats = ['%Y-%m-%d', '%y-%b', '%b-%y', '%m/%d/%Y', '%d-%m-%Y']
    
    for format in date_formats:
        if type(date_str) != str:
            return 'nan'
        try:
            # Try to parse the date string with the current format
            date_obj = datetime.strptime(date_str, format)

            # If the format is already correct, move to the next value.
            if format == '%Y-%m-%d':
                 continue
            
            if format == '%y-%b' or format == '%b-%y':
                return datetime.strftime(date_obj, '%Y-%m-01')
    
            return date_obj.strftime('%Y-%m-%d')

        except ValueError:
                # This exception will happen if the format does not match
                continue

    return "Invalid date format or unknown format."



def get_list_difference(list_1, list_2):
    set1 = set(list_1)
    set2 = set(list_2)
    difference = set1 - set2
    return list(difference)


def generate_file_schema(schema_file_path):
    df = pd.read_csv(schema_file_path)
    dtype_mapping = {}

    for index, row in df.iterrows():
        field_name = row['Field Name']
        dtype = get_type_mapping(row['Data Type'])
        nullable = (row['Nullability'])
        dtype_mapping[field_name] = {
            'data_type': dtype,
            'nullable': nullable
        }
    return dtype_mapping
        
# New Nullable int type in pandas:
# https://pandas.pydata.org/docs/reference/api/pandas.Int64Dtype.html#pandas.Int64Dtype

def get_type_mapping(field, is_pandas=True):
    type_mapping = {
        'Integer': (pd.Int64Dtype() if is_pandas is True else 'int64'),
        'Double': 'float64',
        'String': 'object',  
        'Date': 'object'
    }   
    return type_mapping[field]



def fill_missing_columns(df:pd.DataFrame, schema_file_path:str):
    schema = generate_file_schema(schema_file_path)
    schema_columns = [key for key in schema]
    df_columns = df.columns()
    missing_columns = get_list_difference(schema_columns, df_columns)

    # Fill in missing columns in schema
    for column in missing_columns:
        column_schema = schema_columns[column]
        column_dtype = column_schema['data_type']
        column_nullability = column_dtype['nullable']
        if column_nullability == True:
            df[column] = pd.Series()
        if column_dtype == 'object':
            df[column] = ''
        if column_dtype == 'float':
            df[column] = 0.0
        if column_dtype == 'int':
            df[column] = 0

    return df
    

# Incomplete here. I have this as a one size fits all, 
# but I'd imagine we'd want to be able to pick and choose 
# which rows we drop or fill depending on the column
def handle_nulls(df:pd.DataFrame, schema_file_path:str,delete_rows:bool=True, fill_null_rows:bool=False):
    schema = generate_file_schema(schema_file_path)
    non_null_columns = [key for key in schema if key['nullable'] == False]

    for column in non_null_columns:
        if delete_rows == True:
            df = df[df[column].notna()]
        
        if fill_null_rows == True:

            df[['columns']] = df[['columns']].fillna(value=0)






