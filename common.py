from collections.abc import Generator
from csv import DictReader
from pathlib import Path
import pyap

DB_NAME = "brightwheel"

def csv_data(input_path: Path) -> Generator[dict, None, None]:
    """
    Generator function to iterate through a CSV file
    :param input_path: path to CSV file
    :return:
    """
    with open(input_path, 'r') as input_file:
        dict_reader = DictReader(input_file)
        for input_row in dict_reader:
            yield input_row

def parse_address(address_string: str):
    """
    Parses a string containing address information
    :param address_string:
    :return:
    """
    address_parser = pyap.parse(address_string, country="US")
    if len(address_parser) == 0:
        return None
    else:
        return address_parser[0]

