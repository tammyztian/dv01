import pandas as pd
import dask.dataframe as dd
from typing import Optional
from pydantic import BaseModel
from datetime import datetime

from main.csv_reader import create_dataframe


DEFAULT_UPLOAD_PATH =  f'../data/{datetime.now()}_'
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
def transform_data(file_path, schema_path):
    raw_df = create_dataframe(file_path, schema_path)






def standarize_to_ISO_date(date_str):
    date_formats = ['%Y-%m-%d', '%y-%b', '%b-%y', '%m/%d/%Y', '%d-%m-%Y']
    for format in date_formats:
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


def generate_file_schema(schema_file_path):
    df = pd.read_csv(schema_file_path)
    dtype_mapping = {}

    for row in df.iterrows():
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
# float is nullable in dask.
def get_type_mapping(field, is_pandas=True):
        type_mapping = {
            'Integer': (pd.Int64Dtype() if is_pandas is True else 'float64'),
            'Double': 'float64',
            'String': 'object',  
            'Date': 'object'
        }   
        return type_mapping[field['Data Type']] 




