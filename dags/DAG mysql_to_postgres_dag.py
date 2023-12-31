from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import mysql.connector

default_args = {
    'owner': 'Mohamed-Ali',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 26),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'mysql_to_postgres', default_args=default_args,
    description='Fetch data from MySQL and insert into PostgreSQL',
    schedule_interval=timedelta(days=1),
)

def transfer_data(**kwargs):
    # PostgreSQL connection details
    pg_host = "172.18.0.5"
    pg_port = 5432
    pg_database = "postgres"
    pg_user = "airflow"
    pg_password = "airflow"

    # MySQL connection details
    mysql_host = "172.18.0.3"
    mysql_port = 3306
    mysql_database = "airflow"
    mysql_user = "root"
    mysql_password = "airflow"

    # Connect to MySQL
    mysql_conn = mysql.connector.connect(
        host=mysql_host,
        port=mysql_port,
        database=mysql_database,
        user=mysql_user,
        password=mysql_password
    )
    mysql_cursor = mysql_conn.cursor()

    # Connect to PostgreSQL
    pg_conn = psycopg2.connect(
        host=pg_host,
        port=pg_port,
        database=pg_database,
        user=pg_user,
        password=pg_password
    )
    pg_cursor = pg_conn.cursor()

    # Query to fetch data from MySQL
    mysql_query = "SELECT * FROM mytable;"

    # Execute the query
    mysql_cursor.execute(mysql_query)

    # Fetch all rows
    rows = mysql_cursor.fetchall()

    # Insert each row into PostgreSQL
    for row in rows:
        pg_cursor.execute(f"""
            INSERT INTO mytable (
                _id, gender, name_title, name_first, name_last, location_street_number,
                location_street_name, location_city, location_state, location_country,
                location_postcode, location_coordinates_latitude, location_coordinates_longitude,
                location_timezone_offset, location_timezone_description, email,
                picture_large, picture_medium, picture_thumbnail
            ) VALUES ({', '.join(['%s']*len(row))});
        """, tuple(row))

    pg_conn.commit()

    # Close the connections
    mysql_cursor.close()
    mysql_conn.close()
    pg_cursor.close()
    pg_conn.close()

t1 = PythonOperator(
    task_id='transfer_data',
    python_callable=transfer_data,
    provide_context=True,
    dag=dag,
)
