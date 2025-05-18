# Data-Engineering-Nashville-Accidents
Using Pyspark, DBT, Snowflake, and Apache Airflow


# 🚗 Nashville Accident Reports (Jan 2018 – Apr 2025)

This project explores, cleans, transforms, and visualizes traffic accident data in Nashville from January 2018 to April 2025. The full dataset is publicly available on [Kaggle](https://www.kaggle.com/datasets/justinwilcher/nashville-accident-reports-jan-2018-apl-2025/data).

The project showcases the modern data engineering stack using:
- 🐍 Apache Spark (PySpark) for data preprocessing
- ❄️ Snowflake as the cloud data warehouse
- 🧱 dbt (Data Build Tool) for data transformation
- 🛠️ Apache Airflow for orchestration
- 📊 Power BI for data visualization

---

## 🧹 1. Data Preprocessing (PySpark)

Using Apache Spark, I:
- Explored data types, distinct values, and null distributions
- Identified and removed duplicate records
- Handled numerous missing/null values using appropriate cleaning techniques
- Saved the cleaned data into a `.csv` file for further use

---

## ❄️ 2. Data Loading to Snowflake

Using the Snowflake UI:
- Created a **stage** and uploaded the cleaned `.csv` file
- Defined a **Snowflake table** to ingest the staged data
- Ensured the data was ready for dbt transformations

---

## 🧱 3. Data Transformation (dbt)

### ➤ Source Layer
- Created a **source** to connect dbt with the Snowflake raw table

### ➤ Staging Layer
- Imported the raw dataset
- Renamed columns for consistency and clarity

### ➤ Intermediate Layer
- Created **dimension tables**:
  - `int_dim_date`
  - `int_dim_location`
  - `int_dim_weather`
  - `int_dim_collision`
  - `int_dim_harm`
  - `int_SK_full_table`
- Generated **surrogate keys** for each dimension table

### ➤ Marts Layer
- Built the **fact table** by combining dimensions with measures (e.g., accident severity, date, weather, etc.)

### 📌 Data Lineage View
![DBT Data Lineage](./Data%20Lineage%20DBT.png)

---

## 🛠️ 4. Workflow Orchestration (Apache Airflow)

Built a DAG with the following tasks using `BashOperator`:
1. `dbt deps` – Install dbt packages
2. `dbt run` – Run models
3. `dbt test` – Test transformations
4. `python script` – Download transformed tables from Snowflake locally

### 📌 Airflow DAG
![Airflow DAG Graph](./Airflow%20DAG%20Graph.png)

---

## 📊 5. Data Visualization (Power BI)

Analyzed accident patterns using Power BI by:
- Visualizing the correlation between weather, time of day, location, and accident severity
- Creating multiple insightful and interactive dashboards

### 📌 Power BI Graph 1
![Power BI Graph 1](./Power%20BI%201.png)

### 📌 Power BI Graph 2
![Power BI Graph 2](./Power%20BI%202.png)

### 📌 Power BI Model View
![Power BI Modeling](./Power%20BI%20Modeling.png)

---

## 🧠 Key Learnings

- Hands-on experience integrating multiple tools in a modern data pipeline
- Designing multi-layered data models using dbt best practices
- Using Airflow to automate and monitor data workflows
- Visual storytelling with Power BI dashboards

