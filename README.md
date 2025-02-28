# ETL Pipeline using Python

## This simple datapipe line demonstrate how you can use Python to extract data from CSV, transform the load and store the data in a SQL database of your choice

### Prequisites

* Python >=3.0
* SQL database (Have used PostgreSQL for this Example)

### Installation

* Clone the repository
* Make .env file by copying the .env.example file
* Make virtual env by command `Python3 -m venv .venv`
* Activate virtual environment `source .venv/bin/active`
* Install all dependencies `pip install -r requirements.txt`

### Creating SQL database and table

* Create SQL database using command `psql -U postgres -c "CREATE DATABASE <database_name>;"`
* Create table in database 
```
CREATE TABLE sales (
    id SERIAL PRIMARY KEY,
    product VARCHAR(255) NOT NULL,
    price NUMERIC(10,2) NOT NULL,
    date DATE NOT NULL
);
```

### Executing the program

* `Python index.py`