# Data-Engineering-with-Databricks
Transform data with Spark, Manage Data with Delta Lake, Build Data Pipelines with Delta Live Tables, Manage Data Access with Unity Catalog.

# Notebook 1: 
Transform Data with Spark

%md


## Data Overview

For learning purposes, I worked with a sample of raw Kafka data written as JSON files. 

Each file contains all records consumed during a 5-second interval, stored with the full Kafka schema as a multiple-record JSON file.

| field | type | description |
| --- | --- | --- |
| key | BINARY | The **`user_id`** field is used as the key; this is a unique alphanumeric field that corresponds to session/cookie information |
| value | BINARY | This is the full data payload (to be discussed later), sent as JSON |
| topic | STRING | While the Kafka service hosts multiple topics, only those records from the **`clickstream`** topic are included here |
| partition | INTEGER | 2 partitions (0 and 1) |
| offset | LONG | This is a unique value, monotonically increasing for each partition |
| timestamp | LONG | This timestamp is recorded as milliseconds since epoch, and represents the time at which the producer appends a record to a partition |
