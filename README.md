# Consumer Complaints ETL

## Problem

- Load the consumer complaint data from Kaggle into a SQL database.
- Implement the ability to insert transactions in batches (1 to 1000 rows) with a single request (API or ETL process).
- Include an image with a proposed Azure architecture for production deployment.
- Create a query to get the number of complaints for each product and sub-product for the year 2023, divided by quarter.

## Solution

### 1. Download the Data

First, the consumer complaint dataset is downloaded from Kaggle. The download functionality is encapsulated in a function that fetches the dataset if it is not already available locally. The endpoint could take some time depending on the size of the data.

- **Endpoint**: **POST** `/complaints/download`

### 2. Loading the Data

#### Approach 1: Load All Data at Once

This approach loads the entire dataset in batches into a temporary table and then upserts it into the main table. The entire process is done in chunks (1 to 1000 rows) to manage memory efficiently but is executed in a single request. The endpoint could take some time depending on the size of the data.

- **Endpoint**: **POST** `/complaints/load_all`

#### Approach 2: Load Data in Batches

This approach allows loading the data incrementally (1 to 1000 rows), one batch at a time. It supports pagination, enabling the user to specify the size of each batch and the page to load, making it possible to load and process data in smaller chunks and handle larger datasets over multiple requests.

- **Endpoint**: **POST** `/complaints/load_batch`

## Development

### Up and Running

1. **Create a kaggle.json File**:

   Place the kaggle.json file in the root of the project directory. This file is required for authentication with the Kaggle API.

2. **Build and Run the Docker Containers**:

   ```bash
   docker compose up --build
   ```

3. **Access the Application:**:

   The FastAPI application will be available at http://localhost:8000.

### Core Libraries

- **FastAPI**: Used to create the API endpoints for downloading and loading the dataset.
- **kaggle**: Utilized to download the consumer complaint dataset directly from Kaggle.
- **pandas**: Applied to read the CSV data in batches, transform it, and handle data operations.
- **psycopg2-binary**: This library was used to connect to the PostgreSQL database, enabling the script to execute SQL commands and manage data storage.
- **python-dotenv**: Used to load environment variables from a `.env` file, making it easy to configure database connections and other settings without hardcoding sensitive information.
- **SQLAlchemy**: Provides the ORM for managing database schema and performing database operations such as inserts, updates, and upserts, ensuring efficient and readable database interactions.
- **uvicorn**: Used to serve the FastAPI application, providing a high-performance ASGI server for the API endpoints.

### API Documentation

The project includes interactive API documentation generated automatically by FastAPI. The documentation can be accessed once the server is running.

- **Swagger UI**: Provides a user-friendly interface for exploring and testing the API.
  - Accessible at: [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)
- **ReDoc**: Provides an alternative documentation interface.
  - Accessible at: [http://127.0.0.1:8000/redoc](http://127.0.0.1:8000/redoc)

## Azure Data Architecture Description

The proposed Azure architecture includes the following services:

- **Data Factory**: Orchestrates the entire ETL process and API calls due to the requirement to serve an API.
- **API Management**: Manages and secures API calls, initiating data processing.
- **App Function (Azure Functions)**: Handles data movement by fetching data from Kaggle, saving raw data to Data Lake Gen2, and processed data to SQL Database.
- **Data Lake Gen2**: Stores raw data fetched from Kaggle.
- **SQL Database**: Stores processed data and provides data to Power BI for visualization.
- **Power BI**: Visualizes data stored in the SQL Database with interactive dashboards and reports.

### Additional Tools (Not Included in the Diagram)

These services can be integrated for enhanced security, deployment, and monitoring capabilities:

- **Azure Key Vault**: Securely manages and stores secrets like API keys and connection strings.
- **Azure App Service**: Provides a managed platform for running web apps and APIs.
- **Application Insights**: Monitors application performance and health with real-time analytics and diagnostics.

## SQL Query for Complaints by Product and Subproduct

The following SQL query retrieves the number of complaints for each product and subproduct for the year 2023, divided by quarter, and orders the results alphabetically by product and subproduct:

```sql
SELECT
    product AS Product,
    sub_product AS SubProduct,
    SUM(CASE WHEN EXTRACT(QUARTER FROM date_received) = 1 THEN 1 ELSE 0 END) AS Q1,
    SUM(CASE WHEN EXTRACT(QUARTER FROM date_received) = 2 THEN 1 ELSE 0 END) AS Q2,
    SUM(CASE WHEN EXTRACT(QUARTER FROM date_received) = 3 THEN 1 ELSE 0 END) AS Q3,
    SUM(CASE WHEN EXTRACT(QUARTER FROM date_received) = 4 THEN 1 ELSE 0 END) AS Q4
FROM
    consumer_complaints
WHERE
    EXTRACT(YEAR FROM date_received) = 2023
GROUP BY
    product, sub_product
ORDER BY
    product, sub_product;
```

## Next Steps

The solution can be enhanced further by implementing the following features:

- **Tests Implementation**:
  Use `pytest` to implement tests that verify all functionalities, such as data download, data ingestion, and data upsert processes, ensuring the application works as expected.
- **Flexible Data Extraction**:
  Implement a Factory design pattern for data extraction functions, making it easy to add new types of data sources as modular components that can be tested and maintained separately as business requirements evolve.
- **Robust Data Validation**:
  Integrate a library like `great_expectations` to define and enforce data quality checks, creating a validation layer that catches anomalies and inconsistencies in the data early in the pipeline.
- **Enhanced Error Handling**:
  Add more detailed error codes and messages in the API responses to follow backend standards, providing clearer information to users about the nature of errors and how to resolve them.
