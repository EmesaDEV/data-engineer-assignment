# Take-home Assignment Emesa

## How to run the solution
Prerequisites:
* Docker
* Python3
* Virtualenv package installed
* Make

1. Clone the repo: `git clone https://github.com/JPlanken/emesa-de-assignment.git`
2. cd into repo: `cd emesa-de-assignment`
3. Install required packages into a virtual environment using `make venv`
4. Launch Postgres on Docker by running `make pg_up`
5. Run the pipeline `python3 pipeline.py`
6. Launch the gunicorn server using `make server`
7. Visit `http://127.0.0.1:5000/` in your browser
8. Download the required CSV-file
9. Inspect the data
10. Stop Docker container by running `make pg_down`

## Solution
My solution consists of the following elements:

#### Postgres
I run Postgres on Docker. This so that any user can recreate this solution on his machine. Although Postgres is not quite a scalable cloud DWH-solution like Snowflake it allows me to simulate such an environment on my local machine.

#### ELT-pipeline
For the pipeline I follow an ELT flow. I extract the data from the CSV-file using PySpark. I create a typed Spark DataFrame so that in future loads the pipeline would fail if the schema all of a sudden changes. I initialise the database by executing SQL through Psycopg2. Using the Spark's JDBC I write the dataframe to a table. I define several layers in my 'DWH'; a raw-, dwh- and mrt-layer. In the raw layer I load source data 'as-is'. In the dwh layer I 'model' the data into entities (in this case just an orders table). In the mrt layer I create several data marts. I transform the data by running SQL queries to create views, much like one would do with e.g. DBT. I write the pipeline as one big file, essentially to resemble an Airflow DAG file.

#### Use Case
As a usecase I image that we have a Marketing team that want to data sets. One of customer the bought in the month February or December so that they can be send Valentines or Chrismas promotion materials. The Marketing team wants to be able to download the data in CSV format.

#### Flask API / gunicorn server
In order to serve the data a Flask API is used. By visition the homepage 2 hyperlinks allow the user to download CSV files containing customer data. To do this a 'PostgresConnector' class is used to connect to the Postgres database and query the data.