from pathlib import Path
import datetime
from psycopg2 import Error, sql
from common import DB_NAME, csv_data, parse_address
from db_utils import get_db_connection, build_insert_sql_query


def ingest_source3(file_path: Path) :
    """
    logic for ingesting/transforming source3 data
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

            # check to see address code already exists:
            cursor.execute(
                sql.SQL("SELECT {} FROM {} WHERE {} = %s AND {} = %s AND {} = %s").format(sql.Identifier('addresscode_id'),
                                                                              sql.Identifier('addresscode'),
                                                                              sql.Identifier('city'),
                                                                              sql.Identifier('zip'),
                                                                              sql.Identifier('state')),
                [row["City"], row["Zip"], row["State"]])
            record = cursor.fetchone()

            if record is None:
                current_values = \
                    [
                        row["City"],
                        row["County"],
                        row["State"],
                        row["Zip"],
                        datetime.datetime.now()
                    ]
                cursor.execute(build_insert_sql_query("addresscode"),
                               current_values)

                new_record = cursor.fetchone()
                addresscode_id = new_record[0]
            else:
                addresscode_id = record[0]

            # check to see license already exists:
            current_license_number = row["Facility ID"]
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
                    row["Issue Date"] if len(row["Issue Date"]) > 0 else None,
                    current_license_number,
                    None,
                    row["Type"],
                    row["Operation Name"],
                    datetime.datetime.now()
                ]

                cursor.execute(build_insert_sql_query("license"), current_values)

                new_record = cursor.fetchone()
                license_id = new_record[0]
            else:
                license_id = record[0]

            # check to see if contact already exists
            current_first_name = row["Operation Name"]
            cursor.execute(
                sql.SQL("SELECT {} FROM {} WHERE {} = %s AND {} = %s").format( sql.Identifier('contact_id'),
                                                                            sql.Identifier('contact'),
                                                                            sql.Identifier('first_name'),
                                                                            sql.Identifier('phone')),
                        [current_first_name, row["Phone"]])
            record = cursor.fetchone()

            if record is None:
                current_values = \
                    [
                        current_first_name,
                        None,
                        None,
                        row["Email Address"],
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
                    ",".join(["Infant="+row["Infant"],"Toddler="+row["Toddler"],"Preschool="+row["Preschool"], "School="+row["School"]]),
                    row["Capacity"] if len(row["Capacity"]) > 0 else None,
                    None,
                    row["Address"],
                    None,
                    contact_id,
                    row["Operation Name"],
                    addresscode_id,
                    None,
                    None,
                    license_id,
                    None,
                    None,
                    row["Operation Name"],
                    row["Facility ID"],
                    None,
                    None,
                    row["Type"],
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