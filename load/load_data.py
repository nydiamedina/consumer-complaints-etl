import os
import pandas as pd

from dotenv import load_dotenv
from sqlalchemy import (
    create_engine,
    Table,
    Column,
    Integer,
    String,
    MetaData,
    Date,
    Boolean,
)
from sqlalchemy.exc import SQLAlchemyError


def read_data_in_batches(file_path, batch_size=1000):
    """
    Reads data from a CSV file in batches and renames columns to match SQL table schema.

    :param file_path: The path to the CSV file.
    :param batch_size: The number of rows per batch.
    :return: A generator that yields dataframes of the specified batch size.
    """
    column_mapping = {
        "Date received": "date_received",
        "Product": "product",
        "Sub-product": "sub_product",
        "Issue": "issue",
        "Sub-issue": "sub_issue",
        "Consumer complaint narrative": "consumer_complaint_narrative",
        "Company public response": "company_public_response",
        "Company": "company",
        "State": "state",
        "ZIP code": "zip_code",
        "Tags": "tags",
        "Consumer consent provided?": "consumer_consent_provided",
        "Submitted via": "submitted_via",
        "Date sent to company": "date_sent_to_company",
        "Company response to consumer": "company_response_to_consumer",
        "Timely response?": "timely_response",
        "Consumer disputed?": "consumer_disputed",
        "Complaint ID": "complaint_id",
    }

    for chunk in pd.read_csv(file_path, chunksize=batch_size):
        chunk.rename(columns=column_mapping, inplace=True)
        # Convert Yes/No fields to Boolean
        if "timely_response" in chunk.columns:
            chunk["timely_response"] = chunk["timely_response"].map(
                {"Yes": True, "No": False}
            )
        yield chunk


def insert_data_to_sql(dataframe, table_name, engine, schema=None):
    """
    Inserts a dataframe into an SQL table.

    :param dataframe: The dataframe to insert.
    :param table_name: The name of the table to insert data into.
    :param engine: The SQLAlchemy engine connected to the database.
    :param schema: The schema to use for the table.
    """
    dataframe.to_sql(table_name, engine, if_exists="append", index=False, schema=schema)


def create_table_if_not_exists(engine, table_name, schema=None):
    """
    Creates an SQL table if it doesn't exist.

    :param engine: The SQLAlchemy engine connected to the database.
    :param table_name: The name of the table to create.
    :param schema: The schema to use for the table.
    """
    metadata = MetaData(schema=schema)

    consumer_complaints = Table(
        table_name,
        metadata,
        Column("date_received", Date),
        Column("product", String),
        Column("sub_product", String),
        Column("issue", String),
        Column("sub_issue", String),
        Column("consumer_complaint_narrative", String),
        Column("company_public_response", String),
        Column("company", String),
        Column("state", String),
        Column("zip_code", String),
        Column("tags", String),
        Column("consumer_consent_provided", String),
        Column("submitted_via", String),
        Column("date_sent_to_company", Date),
        Column("company_response_to_consumer", String),
        Column("timely_response", Boolean),
        Column("consumer_disputed", String),
        Column("complaint_id", Integer, primary_key=True),
    )

    try:
        consumer_complaints.create(engine, checkfirst=True)
        print(f"Table {schema}.{table_name} created or already exists.")
    except SQLAlchemyError as e:
        print(f"Error creating table {schema}.{table_name}: {e}")


def drop_table_if_exists(engine, table_name, schema=None):
    """
    Drops an SQL table if it exists.

    :param engine: The SQLAlchemy engine connected to the database.
    :param table_name: The name of the table to drop.
    :param schema: The schema to use for the table.
    """
    metadata = MetaData(schema=schema)
    table = Table(table_name, metadata)

    try:
        table.drop(engine, checkfirst=True)
        print(f"Table {schema}.{table_name} dropped.")
    except SQLAlchemyError as e:
        print(f"Error dropping table {schema}.{table_name}: {e}")


if __name__ == "__main__":
    # Load environment variables from .env file
    load_dotenv()

    # Configuration
    DATA_PATH = "./data/complaints.csv"
    DATABASE_URI = os.getenv("DATABASE_URI")
    TABLE_NAME = "consumer_complaints"
    SCHEMA_NAME = "public"
    BATCH_SIZE = 1000

    # Create a database engine
    engine = create_engine(DATABASE_URI)

    # Drop the table if it exists
    drop_table_if_exists(engine, TABLE_NAME, schema=SCHEMA_NAME)

    # Create the table if it doesn't exist
    create_table_if_not_exists(engine, TABLE_NAME, schema=SCHEMA_NAME)

    # Insert data in batches
    for batch in read_data_in_batches(DATA_PATH, BATCH_SIZE):
        insert_data_to_sql(batch, TABLE_NAME, engine, schema=SCHEMA_NAME)
