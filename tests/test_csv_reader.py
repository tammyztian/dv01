from main.csv_reader import \
    create_dataframe, \
    get_file_size, \
    remove_rows_with_high_null, \
    get_header

import mock
import unittest

SAMPLE_DATA_PATH='main/data/small_sample_loan_data.csv'
SAMPLE_NO_HEADER_DATA_PATH='main/data/no_header_loan_data.csv'
SAMPLE_SCHEMA_PATH='main/data/sample_loan_schema.csv'

class RmTestCase(unittest.TestCase):
   
    def test_has_header_finds_header(self):
        header_index, headers = get_header(SAMPLE_DATA_PATH)
        self.assertEqual(header_index, 2)
        self.assertTrue(headers.__contains__('funded_amnt_inv'))


    def test_has_header_finds_no_header_when_no_header_exists(self):
        header_index, headers = get_header(SAMPLE_NO_HEADER_DATA_PATH)
        self.assertEqual(header_index, 0)
        self.assertFalse(headers.__contains__('funded_amnt_inv'))


    def test_remove_rows_with_high_null(self):
        df = create_dataframe(SAMPLE_DATA_PATH, SAMPLE_SCHEMA_PATH)
        clean_df = remove_rows_with_high_null(df)
        self.assertEqual(clean_df.shape, (5, 150))


    @mock.patch('main.csv_reader.os.path.getsize', return_value=1000000000)    
    def test_get_file_size(self, mock_get_size):
        file_size = get_file_size('some_file')
        print(file_size)
        self.assertTrue(file_size == 0.9313225746154785)


    @mock.patch('main.csv_reader.get_file_size', return_value=10)
    @mock.patch('main.csv_reader.create_pandas_df')
    @mock.patch('main.csv_reader.create_dask_df')
    def test_it_uses_dask_for_large_files_read_csvs(self, mock_get_file_size, mock_create_pandas_df, mock_create_dask_df):
        create_dataframe(SAMPLE_DATA_PATH,SAMPLE_SCHEMA_PATH)
        self.assertFalse(mock_create_pandas_df.called)
        self.assertTrue(mock_create_dask_df.called)
    

    @mock.patch('main.csv_reader.get_file_size', return_value=0)
    @mock.patch('main.csv_reader.create_pandas_df')
    @mock.patch('main.csv_reader.create_dask_df')
    def test_it_uses_pandas_for_small_files_read_csvs(self, mock_get_file_size, mock_create_pandas_df, mock_create_dask_df):
        create_dataframe(SAMPLE_DATA_PATH,SAMPLE_SCHEMA_PATH)
        self.assertTrue(mock_create_pandas_df.called)

        
