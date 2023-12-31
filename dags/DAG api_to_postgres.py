from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import requests
import uuid

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
    'user_data_to_postgres', default_args=default_args,
    description='Fetch user data from API and insert into PostgreSQL',
    schedule_interval=timedelta(days=1),
)

def fetch_and_insert_data(**kwargs):
    # PostgreSQL connection details
    host = "172.18.0.5"
    port = 5432
    database = "postgres"
    user = "airflow"
    password = "airflow"

    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )
    cursor = conn.cursor()

    # API URL
    url = "https://randomuser.me/api/"

    # Number of random users to fetch in each batch
    batch_size = 100

    # Total number of random users to fetch
    total_users = 2000

    # Fetch data from the API in batches
    for offset in range(0, total_users, batch_size):
        # Fetch data from the API
        response = requests.get(f"{url}?results={batch_size}&nat=us,gb,fr")

        if response.status_code == 200:
            # The response is in JSON format. You can access the data using response.json()
            data = response.json()
            # Extract the results, which contains an array of user data
            results = data.get("results", [])

            # Insert each user into the PostgreSQL table
            for user in results:
                # Initialize an empty list to store the column values
                values = []

                # Generate a unique _id for each user
                _id = str(uuid.uuid4())
                values.append(_id)

                # Define the order of columns to ensure they match the INSERT statement
                columns = [
                    '_id', 'gender', 'name_title', 'name_first', 'name_last', 'location_street_number',
                    'location_street_name', 'location_city', 'location_state', 'location_country',
                    'location_postcode', 'location_coordinates_latitude', 'location_coordinates_longitude',
                    'location_timezone_offset', 'location_timezone_description', 'email',
                    'picture_large', 'picture_medium', 'picture_thumbnail'
                ]

                # Iterate over columns and add the corresponding value to the list
                for column in columns[1:]:  # Skip the first column (_id) because we already added it
                    keys = column.split('_')
                    value = user
                    for key in keys:
                        if isinstance(value, dict):
                            value = value.get(key, None)
                    values.append(value)

                # Use the values list in the INSERT statement
                cursor.execute(f"""
                    INSERT INTO public.mytable (
                        {', '.join(columns)}
                    ) VALUES ({', '.join(['%s']*len(values))});
                """, tuple(values))

            conn.commit()
            print(f"Inserted {len(results)} users into PostgreSQL. Offset: {offset}")
        else:
            print("Failed to retrieve data. Status code:", response.status_code)

    # Close the PostgreSQL connection
    cursor.close()
    conn.close()

t1 = PythonOperator(
    task_id='fetch_and_insert_data',
    python_callable=fetch_and_insert_data,
    provide_context=True,
    dag=dag,
)
