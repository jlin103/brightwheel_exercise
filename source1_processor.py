from pathlib import Path
import datetime
from psycopg2 import Error, sql
from common import DB_NAME, csv_data, parse_address
from db_utils import get_db_connection, build_insert_sql_query

def ingest_source1(file_path: Path) :
    """
    logic for ingesting/transforming source1 data
    :param file_path: path to source csv file
    :return:
    """
    connection = None
    cursor = None

    try:
        # Connect to an existing database
        connection = get_db_connection(DB_NAME)

        # Create a cursor to perform database operations
        cursor = connection.cursor()

        for row in csv_data(file_path):

            # populate addresscode table
            current_address = parse_address(row["Address"])
            current_city, current_postal_code = None, None

            if current_address is not None:
                current_city = current_address.city
                current_postal_code = current_address.postal_code

            # check to see address code already exists:
            cursor.execute(
                sql.SQL("SELECT {} FROM {} WHERE {} = %s AND {} = %s AND {} = %s").format(sql.Identifier('addresscode_id'),
                                                                              sql.Identifier('addresscode'),
                                                                              sql.Identifier('city'),
                                                                              sql.Identifier('zip'),
                                                                              sql.Identifier('county')),
                [current_city, current_postal_code, row["County"]])
            record = cursor.fetchone()

            if record is None:
                current_values = \
                    [
                        current_city,
                        row["County"],
                        row["State"],
                        current_postal_code,
                        datetime.datetime.now()
                    ]
                cursor.execute(build_insert_sql_query("addresscode"),
                               current_values)

                new_record = cursor.fetchone()
                addresscode_id = new_record[0]
            else:
                addresscode_id = record[0]

            # check to see license already exists:
            current_license_number = "".join([c for c in row["Credential Number"] if c.isdigit()])
            cursor.execute(
                sql.SQL("SELECT {} FROM {} WHERE {} = %s").format(sql.Identifier('license_id'), \
                                                                              sql.Identifier('license'),
                                                                              sql.Identifier('license_number')),
                [current_license_number])
            record = cursor.fetchone()

            if record is None:
                current_values = \
                [
                    row["Status"],
                    row["First Issue Date"],
                    current_license_number,
                    None,
                    row["Credential Type"],
                    row["Primary Contact Name"],
                    datetime.datetime.now()
                ]

                cursor.execute(build_insert_sql_query("license"), current_values)

                new_record = cursor.fetchone()
                license_id = new_record[0]
            else:
                license_id = record[0]

            # check to see if contact already exists
            current_first_name = row["Primary Contact Name"].split(" ")[0]
            current_last_name = row["Primary Contact Name"].split(" ")[1]
            cursor.execute(
                sql.SQL("SELECT {} FROM {} WHERE {} = %s AND {} = %s AND {} = %s").format( sql.Identifier('contact_id'),
                                                                            sql.Identifier('contact'),
                                                                            sql.Identifier('first_name'),
                                                                            sql.Identifier('last_name'),
                                                                            sql.Identifier('phone')),
                        [current_first_name, current_last_name, row["Phone"]])
            record = cursor.fetchone()

            if record is None:
                current_values = \
                    [
                        current_first_name,
                        current_last_name,
                        row["Primary Contact Role"],
                        None,
                        row["Phone"],
                        None,
                        datetime.datetime.now()
                    ]

                cursor.execute(build_insert_sql_query("contact"), current_values)

                new_record = cursor.fetchone()
                contact_id = new_record[0]
            else:
                contact_id = record[0]

            # insert into facility table
            current_values = \
                [
                    None,
                    None,
                    None,
                    row["Expiration Date"],
                    current_address.full_street if current_address is not None else None,
                    None,
                    contact_id,
                    row["Name"],
                    addresscode_id,
                    None,
                    None,
                    license_id,
                    None,
                    None,
                    row["Primary Contact Name"],
                    None,
                    None,
                    None,
                    row["Credential Type"],
                    datetime.datetime.now()
                ]

            cursor.execute(build_insert_sql_query("facility"), current_values)

            connection.commit()

    except (Exception, Error) as error:
        print("Error: ", error)
    finally:
        if connection:
            cursor.close()
            connection.close()