from main.csv_reader import create_dataframe
from main.parquet_transformer import standardize_ISO_date_columns

CSV_PATH='main/data/sample_loan_data.csv'
SCHEMA_PATH='main/data/sample_loan_schema.csv'

my_df = create_dataframe(CSV_PATH, SCHEMA_PATH)
my_df_ISO_date = standardize_ISO_date_columns(my_df, ['settlement_date'])

print(my_df_ISO_date['settlement_date'].unique())
# print(my_df.head())



