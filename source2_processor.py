from pathlib import Path
import datetime
from psycopg2 import Error, sql
from common import DB_NAME, csv_data, parse_address
from db_utils import get_db_connection, build_insert_sql_query


def ingest_source2(file_path: Path) :
    """
    logic for ingesting/transforming source2 data
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
                        None,
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
            current_license_number = row["Type License"].split(" - ")[1][2:]
            cursor.execute(
                sql.SQL("SELECT {} FROM {} WHERE {} = %s").format(sql.Identifier('license_id'), \
                                                                              sql.Identifier('license'),
                                                                              sql.Identifier('license_number')),
                [current_license_number])
            record = cursor.fetchone()

            if record is None:
                current_values = \
                [
                    None,
                    row["License Monitoring Since"][len("Monitoring Since "):],
                    current_license_number,
                    None,
                    row["Type License"].split(" - ")[0],
                    row["Primary Caregiver"][0:19],
                    datetime.datetime.now()
                ]

                cursor.execute(build_insert_sql_query("license"), current_values)

                new_record = cursor.fetchone()
                license_id = new_record[0]
            else:
                license_id = record[0]

            # check to see if contact already exists
            current_first_name = row["Primary Caregiver"][0:19].split(" ")[0]
            current_last_name = row["Primary Caregiver"][0:19].split(" ")[1]
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
                        None,
                        row["Email"],
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
                    "Y" if len(row["Accepts Subsidy"]) !=0 else "N",
                    ",".join([row["Ages Accepted 1"], row["AA2"], row["AA3"], row["AA4"]]),
                    row["Total Cap"],
                    None,
                    row["Address1"],
                    row["Address2"],
                    contact_id,
                    row["Company"],
                    addresscode_id,
                    row["Star Level"],
                    None,
                    license_id,
                    None,
                    None,
                    row["Primary Caregiver"],
                    None,
                    ",".join([row["Mon"], row["Tues"], row["Wed"], row["Thurs"], row["Friday"], row["Saturday"],
                              row["Sunday"]]),
                    None,
                    row["Type License"].split(" - ")[0],
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