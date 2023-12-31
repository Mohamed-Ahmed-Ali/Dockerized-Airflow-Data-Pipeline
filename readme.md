# Dockerized Airflow Data Pipeline

## Description
This project involves the development of a Dockerized Apache Airflow data pipeline that fetches user data from two different sources and inserts it into a PostgreSQL database. The first data source is the RandomUser API, and the second is an existing MySQL database. The pipeline consists of two separate DAGs, one for each data source, and utilizes PythonOperator tasks to execute the data transfer operations.

## Requirements
- Docker
- Docker Compose
- Apache Airflow
- PostgreSQL
- MySQL

## Technology Stack
- Apache Airflow
- PostgreSQL
- MySQL
- Docker

## Data Source
1. **RandomUser API**: Used to fetch random user data.
2. **MySQL Database**: Existing database containing user data.

## Architecture
![Architecture](./image/image.png)
The architecture involves two separate DAGs within Apache Airflow, each responsible for fetching and transferring data from one source to the PostgreSQL database.

## Data Modeling
The data is modeled in a PostgreSQL table named `mytable`. The schema includes various user attributes such as `_id`, `gender`, `name_title`, `name_first`, `name_last`, etc.

## Deploy the Solution
1. Clone the repository.
2. Navigate to the project directory.
3. Run `docker-compose up` to start the Docker containers for Airflow, PostgreSQL, and MySQL.

## Step-by-Step Guide
1. **RandomUser API to PostgreSQL**
   - DAG Name: `user_data_to_postgres`
   - Task Name: `fetch_and_insert_data`
   - The task fetches random user data from the RandomUser API and inserts it into the PostgreSQL database.

2. **MySQL to PostgreSQL**
   - DAG Name: `mysql_to_postgres`
   - Task Name: `transfer_data`
   - The task transfers data from the MySQL database to the PostgreSQL database.

## Usage
1. Access the Airflow web UI at `localhost:8080`.
2. Trigger the DAGs manually or set up a schedule for automated execution.

## Screenshots
- Include screenshots of the Airflow web UI showing the DAGs and task execution logs.
![Api DAG](./image/api.png)
![MySQL DAG](./image/mysql.png)
![PgAdmin](./image/pgadmin.png)

## Contributing
If you'd like to contribute to the project, please follow the standard GitHub flow:
1. Fork the repository.
2. Create a new branch for your feature or bug fix.
3. Make your changes and submit a pull request.


**Note**: Ensure that the necessary environment variables, such as database connection details, are properly configured in the Airflow environment.


image.png
