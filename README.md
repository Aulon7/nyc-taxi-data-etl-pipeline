## NYC Taxi Data ETL Pipeline

End-to-end data pipeline for NYC Yellow Taxi 2024 data using Spark, GCP, dbt, Airflow and PowerBI.

## Data Pipeline Architecture Flow

1. **Orchestration & Ingestion (E & L)** : Airflow manages the scheduled orchestration for Dataproc (Spark) jobs. Spark reads raw data from Cloud Storage, performs initial processing (Extract/Transform), and pushes partitioned parquet files to BigQuery
2. **Transformation & Modeling (T)**: dbt executes complex transformation logic directly within BigQuery. This phase models staging tables into the final Data Mart (fact and dimensional models), ensuring data is clean and ready for analytics.
3. **Visualization**: Microsoft Power BI connects to the optimized analytics tables in BigQuery, allowed for direct, real-time querying to generate visualizations.

   ![Data Pipeline Architecture](power-bi\src\etl-pipeline.png)

## Data modeling inside Power BI

   ![Data Modeling and Relationships](power-bi\src\model-view.png)

## Airflow DAG

   ![Airflow DAG](power-bi\src\airflow-dag.png)
### Data source
- Parquet files with raw data coming from official NYC TLC Yellow Trips Records (2024): 
(https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- Additional public dataset coming from BigQuery as (`bigquery-public-data`.new_york_taxi_trips.taxi_zone_geom)

### Stack

- **Orchestration**: Apache Airflow 
- **Processing**: Apache Spark on GCP Dataproc Cluster
- **Transformation**: dbt (fact and dimensional models, testing and loading into BigQuery)
- **Storage**: Google Cloud Storage + BigQuery
- **Visualization**: Power BI
- **Deployment**: Docker Compose


### Project Structure

```
├── airflow/          # Orchestration
├── spark/            # Data processing
├── dbt/              # SQL transformations
├── power-bi/         # Data Visualizations
├── config/           # Credentials & configs
└── docker-compose.yml
```


