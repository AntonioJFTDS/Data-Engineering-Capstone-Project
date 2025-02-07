# Data Engineering Capstone Project

## Project Overview
This project creates a **fact/dimension database** using **Apache Spark**. The **fact table** is the **I94 immigration dataset**, while **dimension tables** include **weather and economic data** from a person's country before arriving in the U.S. and their state of residency in the U.S.

## ETL Process
The **ETL pipeline** successfully transforms data into the final model. **Data quality checks** confirm that the row count from the **fact table** and **dimension tables** aligns.

## Technology Choice
- **PySpark** is used for its efficiency in processing large datasets.
- **AWS EMR** is recommended if data volume increases significantly, allowing for scalable Spark pipelines.
- **Apache Airflow** should be used if the pipeline needs to update a **daily dashboard** before **7 AM**.
- **AWS Redshift** is ideal for supporting **100+ users** accessing the database.

## Data Characteristics & Update Frequency
- **Weather and economic data** are more valuable when analyzed **monthly** rather than daily.
- The **I94 immigration dataset** is updated **monthly** (as seen on [trade.gov](https://www.trade.gov/i-94-arrivals-program)).
- The database should be **updated monthly** to match the immigration data release cycle.
