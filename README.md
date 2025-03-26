Streaming Data Pipeline using RandomUser API, Kafka, PostgreSQL, and Cassandra  

This project builds a streaming data pipeline with RandomUser API as the data source, PostgreSQL for temporary storage and validation, Apache Kafka for real-time data streaming, and Apache Cassandra as a scalable NoSQL database for final storage.  
  
Streaming-Data-Pipeline/
│── venv/                           # Folder for virtual environment
│── kafka-psql-csdr.py               # Python script file

Data Flow Overview
![image](https://github.com/takdirzd/basic-streaming/blob/main/basic-streaming.png)

This diagram represents the ETL (Extract, Transform, Load) data pipeline using Apache Airflow for orchestrating data movement across different layers:

+ Data Source - RandomUser API
    - Data is fetched from the RandomUser API using Python.  
    - The retrieved data includes user information such as name, address, email, and other details.  

+ Validation & Backup - PostgreSQL
    - The fetched data is temporarily stored in PostgreSQL for validation.  
    - PostgreSQL serves as a backup, and invalid data is not forwarded to Kafka.  

DATA STREAMING.pdf for details.
