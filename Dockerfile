FROM apache/airflow:2.10.3-python3.9

USER airflow
RUN pip install dbt-core dbt-postgres
RUN python -m pip install dbt-core dbt-snowflake
RUN pip install apache-airflow-providers-apache-spark
RUN pip uninstall protobuf -y
RUN pip install --upgrade --force-reinstall protobuf
RUN pip install snowflake-connector-python pandas
RUN pip install python-dotenv
