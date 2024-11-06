# System Design

The Task2-architecture.png file shows the current batch processing system architecture and future near real-time Spark Streaming processing system architecture

Currently, the batch processing system design uses Airflow to trigger a daily cron job for a fetcher job using AWS Fargate to fetch data from the source data system, AWS Aurora DB and generate CSV source files for the data in the past 24 hours. Within the same Airflow dag and after the fetcher job finishes, the Spark ETL job starts to run using either AWS EMR Serverless Cluster or AWS Glue depending on the size of the job and cost considerations. The Spark ETL job will generate the final feature data and writes it to the data lake in AWS S3 bucket.

The CDC events published by the Kafka topic will be subscribed in near real-time by the Spark Streaming job run in AWS EMR. The Spark Streaming job will process the event in very low latency to perform transformations with CDC like SCD2 etc. The datalake stores feature data in delta format to enable lake house functions like merge, update etc.

