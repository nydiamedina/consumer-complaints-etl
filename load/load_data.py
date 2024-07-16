import pandas as pd

from dotenv import load_dotenv
from sqlalchemy import (
    create_engine,
    text,
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
    except SQLAlchemyError as e:
        raise RuntimeError(f"Error creating table {schema}.{table_name}: {e}")


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
    except SQLAlchemyError as e:
        raise RuntimeError(f"Error dropping table {schema}.{table_name}: {e}")


def upsert_data_from_temp_table(engine, main_table_name, temp_table_name, schema=None):
    """
    Upserts data from the temporary table to the main table using the ON CONFLICT DO UPDATE statement.

    :param engine: The SQLAlchemy engine connected to the database.
    :param main_table_name: The name of the main table to upsert data into.
    :param temp_table_name: The name of the temporary table containing the data.
    :param schema: The schema to use for the tables.
    """
    upsert_sql = text(
        f"""
    INSERT INTO {schema}.{main_table_name} (
        date_received, product, sub_product, issue, sub_issue, 
        consumer_complaint_narrative, company_public_response, company, state, 
        zip_code, tags, consumer_consent_provided, submitted_via, 
        date_sent_to_company, company_response_to_consumer, timely_response, 
        consumer_disputed, complaint_id
    )
    SELECT 
        date_received, product, sub_product, issue, sub_issue, 
        consumer_complaint_narrative, company_public_response, company, state, 
        zip_code, tags, consumer_consent_provided, submitted_via, 
        date_sent_to_company, company_response_to_consumer, timely_response, 
        consumer_disputed, complaint_id
    FROM (
        SELECT DISTINCT ON (complaint_id) * FROM {schema}.{temp_table_name}
    ) AS temp
    ON CONFLICT (complaint_id) DO UPDATE SET
        date_received = EXCLUDED.date_received,
        product = EXCLUDED.product,
        sub_product = EXCLUDED.sub_product,
        issue = EXCLUDED.issue,
        sub_issue = EXCLUDED.sub_issue,
        consumer_complaint_narrative = EXCLUDED.consumer_complaint_narrative,
        company_public_response = EXCLUDED.company_public_response,
        company = EXCLUDED.company,
        state = EXCLUDED.state,
        zip_code = EXCLUDED.zip_code,
        tags = EXCLUDED.tags,
        consumer_consent_provided = EXCLUDED.consumer_consent_provided,
        submitted_via = EXCLUDED.submitted_via,
        date_sent_to_company = EXCLUDED.date_sent_to_company,
        company_response_to_consumer = EXCLUDED.company_response_to_consumer,
        timely_response = EXCLUDED.timely_response,
        consumer_disputed = EXCLUDED.consumer_disputed;
    """
    )

    try:
        with engine.connect() as conn:
            conn.execute(upsert_sql)
            conn.commit()
    except SQLAlchemyError as e:
        raise RuntimeError(f"Error during upsert: {e}")


def load_all_data_to_database(
    data_path, database_uri, main_table_name, temp_table_name, schema_name, batch_size
):
    # Create a database engine
    engine = create_engine(database_uri)

    # Drop the temporary table if it exists
    drop_table_if_exists(engine, temp_table_name, schema=schema_name)

    # Create the main table if it doesn't exist
    create_table_if_not_exists(engine, main_table_name, schema=schema_name)

    # Create the temporary table if it doesn't exist
    create_table_if_not_exists(engine, temp_table_name, schema=schema_name)

    # Insert data in batches into the temporary table
    for batch in read_data_in_batches(data_path, batch_size):
        insert_data_to_sql(batch, temp_table_name, engine, schema=schema_name)

    # Upsert data from the temporary table to the main table
    upsert_data_from_temp_table(
        engine, main_table_name, temp_table_name, schema=schema_name
    )

    # Drop the temporary table after upsert
    drop_table_if_exists(engine, temp_table_name, schema=schema_name)
