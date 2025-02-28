import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Database connection parameters (loaded from .env)
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
}

# Function to extract data from CSV
def extract(file_path):
    try:
        return pd.read_csv(file_path)
    except Exception as e:
        print(f"Error during extraction: {e}")
        return None


# Function to transform data (pure function, does not modify original dataframe)
def transform(df):
    try:
        if df is None or df.empty:
            raise ValueError("Dataframe is empty or None")

        return (
            df.dropna()  # Remove rows with missing values
            .assign(date=lambda x: pd.to_datetime(x['date'], format='%Y-%m-%d'))  # Standardize date format
        )
    except Exception as e:
        print(f"Error during transformation: {e}")
        return None


# Function to load data into PostgreSQL
def load(df, db_config):
    try:
        if df is None or df.empty:
            raise ValueError("Dataframe is empty or None")

        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()

        # Convert DataFrame to a list of tuples for bulk insertion
        records = list(df.itertuples(index=False, name=None))

        query = "INSERT INTO sales (id, product, price, date) VALUES %s"
        execute_values(cur, query, records)

        conn.commit()
        cur.close()
        conn.close()
        print("Data successfully loaded into the database.")
    except Exception as e:
        print(f"Error during loading: {e}")


# Main function to orchestrate ETL pipeline
def etl_pipeline(file_path, db_config):
    try:
        print("Starting ETL pipeline...")
        data = extract(file_path)
        transformed_data = transform(data)
        load(transformed_data, db_config)
        print("ETL pipeline completed successfully.")
    except Exception as e:
        print(f"Error in ETL pipeline: {e}")

# Run ETL pipeline
if __name__ == "__main__":
    etl_pipeline("mock_sales_data.csv", DB_CONFIG)