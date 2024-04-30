from main import csv_reader

def test_read_csvs():
    # Amsterdam to Berlin
    assert csv_reader.read_csvs(
        4.895168, 52.370216, 13.404954, 52.520008
    ) == 576.6625818456291

def test_parse_schema():
    pass

def test_check_file_size():
    pass

def test_has_header():
    pass