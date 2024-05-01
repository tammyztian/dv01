import os
import csv

import pandas as pd
import numpy as np
import dask.dataframe as dd
from typing import Optional
from pydantic import BaseModel
from datetime import datetime


# Language: Scala / Python
# Functionality: This module allows data engineers to read single or multiple CSV files into a designated data processing system.


# Features:
# Adaptive scalability catering to file size (1 MB to 10 GB) and quantity (up to 5000 files).
#   - Are these files all part of one type of dataset, or are they different files for different datasets?
# Auto-detection of header presence in CSV files.
# Support for user-provided headers in cases where files lack headers.
# Capable of handling different file schemas. 
#   - wouldn't the schema have the header values? 

DEFAULT_UPLOAD_PATH =  f'../data/raw/{datetime.now()}'

# file size threshold in GB
FILE_SIZE_THRESHOLD = 10
HARD_CODED_FOOTER = 4
REMOVE_NULL_THRESHOLD = .5

class Dataset(BaseModel):
    file_path: str 
    schema_path: Optional[str] = None
    header: Optional[list] = None
   

def create_dataframe(file_path:str, schema_file_path:str, column_names:list[str]=[]):
    header_row, header = get_header(file_path)
    print(header_row, header)
    file_size = get_file_size(file_path)
    if file_size < FILE_SIZE_THRESHOLD:
        #pandas_schema_map = create_schema_dict(schema_file_path)
        df = create_pandas_df(column_names, header, header_row, file_path)
        return df
    
    if file_size >= FILE_SIZE_THRESHOLD:
        #schema_map = create_dask_schema_dict(schema_file_path)
        df = create_dask_df(column_names, header, header_row, file_path)
        return df

def create_dask_df(column_names, header, header_row, file_path):
    #start_time = datetime.now() 
    if len(column_names) != 0 and len(header) == 0:
            df = dd.read_csv(file_path, skiprows=header_row, names=column_names)
    df = dd.read_csv(file_path, skiprows=header_row)
    #dask_time_elapsed = datetime.now() - start_time 
    # print(dask_time_elapsed)
    df_cleaned = remove_rows_with_high_null(df)
    return df_cleaned

def create_pandas_df(column_names, header, header_row, file_path):
    # pd_start_time = datetime.now() 
    if len(column_names) != 0 and len(header) == 0:
        df = pd.read_csv(file_path, skiprows=header_row, names=column_names)
        
    df =  pd.read_csv(file_path, skiprows=header_row)
    # pandas_time_elapsed = datetime.now() - pd_start_time 
    # print(pandas_time_elapsed)
    df_cleaned = remove_rows_with_high_null(df)
    return df_cleaned

    
def save_raw_dataframe_to_parquet(df: pd.DataFrame, file_path: str):
    # find the last '.' in a string and remove everything after it
    clean_file_name = file_path[:file_path.rfind('.')]
    file_name = f'{DEFAULT_UPLOAD_PATH}_{clean_file_name}.snappy'
    df.to_parquet(file_name)
    return print(f'{file_name} saved.')


def remove_rows_with_high_null(df, null_threshold=REMOVE_NULL_THRESHOLD):
    """
    Takes in a DataFrame where the percentage of null values exceeds the threshold
    and returns the cleaned dataframe.

    """
    # Calculate the percentage of null values in each row
    null_percentages = df.isnull().mean(axis=1)
    # Filter out rows where the percentage of null values exceeds the threshold
    df_cleaned = df[null_percentages <= null_threshold]
    return df_cleaned
    

# Gigabyte conversion: https://stackoverflow.com/questions/5194057/better-way-to-convert-file-sizes-in-python
# should do some testing to see the limits of the infrastructure. 
# I arbitrarily picked 1GB. We could stop the loop once the total is > 1GB with FILE_SIZE_THRESHOLD
def get_file_size(file_path: str):
    '''Takes in a list file path and returns the total size of the files in GB.'''
    
    try:
        # Get the file size in bytes
        size_bytes = os.path.getsize(file_path)
        # Convert bytes to gigabytes
        file_size_gb = size_bytes / (1024 ** 3)
        print(f"File: {file_path}, Size: {file_size_gb} GB")
        return file_size_gb

    except FileNotFoundError:
        print(f"Error: The file '{file_path}' does not exist.")
        return None


### thinking of moving this to parquet transformer instead
# def create_schema_dict(schema_file_path):
#     type_mapping = {
#         # using a float for now to allow for null values, will be able to clean it in transformer
#         'Integer': float,
#         'Double': 'float',
#         'String': 'object',  # Pandas uses 'object' dtype for string data
#         'Date': 'object'
#     }
#     df = pd.read_csv(schema_file_path)
#     dtype_dict = {row['Field Name']: type_mapping[row['Data Type']] for index, row in df.iterrows()}
#     return dtype_dict


# def create_dask_schema_dict(schema_file_path):
#     type_mapping = {
#         'Integer': 'float64',
#         'Double': 'float64',
#         'String': 'object',  
#         'Date': 'object'
#     }
#     df = pd.read_csv(schema_file_path)
#     dtype_dict = {row['Field Name']: type_mapping[row['Data Type']] for index, row in df.iterrows()}
#     return dtype_dict


def get_header(file_path, header_scan_limit=25):
    '''
    Takes in a file path and looks for the header row based on the header scan limit. 
    If there is a header withing the limit, the row index and header will be returned. 
    If not, None, and an empty list is returned. 
    '''
    with open(file_path, 'r', newline='') as file:
        reader = csv.reader(file)
        header_index = 0
        header_row = []

        for index, row in enumerate(reader):
            if index > header_scan_limit:
                print(f'No header found in the first {header_scan_limit} rows of the CSV.')
                return header_index, header_row

            column_list = [item for item in row] 
            # Assuming column names are be unique, if not, then this row is not the header
            unique_columns = set(column_list)
            if len(unique_columns) != len(column_list):
                continue
            
            # If all columns are unique, then check if the column contains strings, 
            # assuming that all header values are some string value, therefore cannot be converted to a num. 
            number_only_column_names = [item for item in row if (item.replace('.', '', 1).isdigit() and item.isnumeric() == True)]
            if len(number_only_column_names) > 0:
                continue
            else:
                header_index = index
                header_row = row
        return header_index, header_row


    