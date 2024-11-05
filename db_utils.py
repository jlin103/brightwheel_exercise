import psycopg2
from psycopg2 import Error, sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.sql import Composed
from table_schemas import addresscode_schema, license_schema, contact_schema, facility_schema


def get_db_connection(db_name: str = ""):
    """
    Helper function to create a PostgreSQL database connection
    :param db_name:
    :return:
    """

    # TODO
    # update postgreSQL connection settings
    return psycopg2.connect(user="<USER_NAME>",
                              password="<PASSWORD>",
                              host="<HOSTNAME>",
                              port="<PORT NUMBER>",
                             database=db_name)

def create_database(db_name: str):
    """
    Helper function to create a PostgreSQL database
    :param db_name: database name
    :return:
    """
    connection = None
    cursor = None
    try:
        connection = get_db_connection()
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        # Create a cursor to perform database operations
        cursor = connection.cursor()

        # check to see if database has already been created
        cursor.execute(sql.SQL("select * from {} where {} = %s").format(sql.Identifier('pg_database'), sql.Identifier('datname')), [db_name])
        record = cursor.fetchone()

        if record is None:
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))

    except (Exception, Error) as error:
        print("Error: ", error)
    finally:
        if connection:
            cursor.close()
            connection.close()


def create_tables(db_name: str):
    """
    Helper function to create the required PostgreSQL tables
    :param db_name:
    :return:
    """
    connection = None
    cursor = None
    try:
        connection = get_db_connection(db_name)
        cursor = connection.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS AddressCode (addresscode_id SERIAL PRIMARY KEY,
                                                    city VARCHAR,                                                                                                  
                                                    county VARCHAR,
                                                    state VARCHAR,
                                                    zip VARCHAR,
                                                    updated_dt TIMESTAMP);"""
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS License (license_id SERIAL PRIMARY KEY,
                                                 license_status VARCHAR,                                                                                                  
                                                 license_issued DATE,  
                                                 license_number NUMERIC,
                                                 license_renewed DATE,  
                                                 license_type VARCHAR,
                                                 licensee_name VARCHAR,
                                                 updated_dt TIMESTAMP);"""
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS Contact (contact_id SERIAL PRIMARY KEY,
                                                first_name VARCHAR,
                                                last_name VARCHAR,
                                                title VARCHAR,
                                                email VARCHAR,     
                                                phone VARCHAR,
                                                phone2 VARCHAR,                                   
                                                updated_dt TIMESTAMP);"""
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS Facility (facility_id SERIAL PRIMARY KEY,
                                                 accepts_financial_aid VARCHAR,
                                                 ages_served VARCHAR,
                                                 capacity NUMERIC,
                                                 certificate_expiration_date DATE,
                                                 address1 VARCHAR,
                                                 address2 VARCHAR,
                                                 contact_id INTEGER,
                                                 company VARCHAR,                                            
                                                 addresscode_id INTEGER,
                                                 curriculum_type VARCHAR,
                                                 language VARCHAR,                                                 
                                                 license_id INTEGER,
                                                 max_age NUMERIC,
                                                 min_age NUMERIC,
                                                 operator VARCHAR,
                                                 provider_id VARCHAR,
                                                 schedule VARCHAR,                                                
                                                 website_address VARCHAR,
                                                 facility_type VARCHAR,
                                                 updated_dt TIMESTAMP,                                                                                                 

                                                CONSTRAINT FK_facility_contact FOREIGN KEY(contact_id)
                                                REFERENCES Contact(contact_id),

                                                CONSTRAINT FK_facility_address_code FOREIGN KEY(addresscode_id)
                                                REFERENCES AddressCode(addresscode_id),

                                                CONSTRAINT FK_facility_license FOREIGN KEY(license_id)
                                                REFERENCES License(license_id));"""
        )

        connection.commit()

    except (Exception, Error) as error:
        print("Error: ", error)
    finally:
        if connection:
            cursor.close()
            connection.close()

def build_insert_sql_query(table_name: str) -> Composed:
    """
    Helper function to generate INSERT SQL statements
    :param table_name:
    :return:
    """
    table_schema = None

    match table_name:
        case "addresscode":
            table_schema = addresscode_schema
        case "license":
            table_schema = license_schema
        case "contact":
            table_schema = contact_schema
        case "facility":
            table_schema = facility_schema

    if table_schema is not None:
        schema_columns = ["{}" for _ in table_schema]
        sql_identifiers_placeholders = ",".join(schema_columns)

        sql_identifiers = [sql.Identifier(table_name)]
        sql_identifiers.extend([sql.Identifier(column_name) for column_name in table_schema])

        values_placeholders = ",".join(["%s" for _ in table_schema])

        return sql.SQL("INSERT INTO {} (" + sql_identifiers_placeholders + ") VALUES (" + values_placeholders + ") RETURNING " + table_name + "_id").format(*sql_identifiers)