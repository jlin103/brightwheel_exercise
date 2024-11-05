import argparse
from common import DB_NAME
from db_utils import create_database, create_tables
from pathlib import Path
from source1_processor import ingest_source1
from source2_processor import ingest_source2
from source3_processor import ingest_source3

# dict mapping source files to processor method
sourcefile_processor = {
    "source1.csv": ingest_source1,
    "source2.csv": ingest_source2,
    "source3.csv": ingest_source3
}

def process_input_data(db_name: str, file_path: Path, file_names: list[str]):
    """
    Entrypoint for processing source CSV files.
    Invokes a source-specific method for processing the file
    :param db_name: database name
    :param file_path: path to source CSV files
    :param file_names: list of CSV file names
    :return:
    """
    for file in file_names:
        sourcefile_processor[file](file_path / file)

if __name__ == '__main__':
    argument_parser = argparse.ArgumentParser()
    argument_parser.add_argument('-p', '--inputpath', type=Path, help='<Required> path to input files', required=True)
    argument_parser.add_argument('-f','--inputfiles', nargs='+', help='<Required> list of input files', required=True)

    create_database(DB_NAME)
    create_tables(DB_NAME)

    # INPUT_PATH_BASE = 'input_files'
    # input_files = ['source1.csv', 'source2.csv', 'source3.csv']

    INPUT_PATH_BASE = argument_parser.parse_args().inputpath
    input_files = argument_parser.parse_args().inputfiles
    input_files_path = Path( Path.cwd() / INPUT_PATH_BASE)

    process_input_data(DB_NAME, input_files_path, input_files)