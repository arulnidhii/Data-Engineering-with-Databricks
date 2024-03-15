print(DA.paths.kafka_events)

files = dbutils.fs.ls(DA.paths.kafka_events)
display(files)

# COMMAND ----------

# DBTITLE 0,--i18n-9abfecfc-df3f-4697-8880-bd3f0b58a864
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Query a Single File
# MAGIC
# MAGIC To query the data contained in a single file

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM json.`${DA.paths.kafka_events}/001.json`

# COMMAND ----------

# DBTITLE 0,--i18n-0f45ecb7-4024-4798-a9b8-e46ac939b2f7
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Query a Directory of Files

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM json.`${DA.paths.kafka_events}`

# COMMAND ----------

# DBTITLE 0,--i18n-035ddfa2-76af-4e5e-a387-71f26f8c7f76
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Create References to Files
# MAGIC This additional Spark logic can be chained to queries against files.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW event_view
# MAGIC AS SELECT * FROM json.`${DA.paths.kafka_events}`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM event_view

# COMMAND ----------

# DBTITLE 0,--i18n-efd0c0fc-5346-4275-b083-4ee96ce8a852
# MAGIC %md
# MAGIC ## Create Temporary References to Files
# MAGIC
# MAGIC Temporary views similarly alias queries to a name that's easier to reference in later queries.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW events_temp_view
# MAGIC AS SELECT * FROM json.`${DA.paths.kafka_events}`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM events_temp_view

# COMMAND ----------

# DBTITLE 0,--i18n-dcfaeef2-0c3b-4782-90a6-5e0332dba614
# MAGIC %md
# MAGIC ## CTEs for Reference within a Query 
# MAGIC Common table expressions (CTEs) are perfect when you want a short-lived, human-readable reference to the results of a query.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cte_json
# MAGIC AS (SELECT * FROM json.`${DA.paths.kafka_events}`)
# MAGIC SELECT * FROM cte_json

# COMMAND ----------

# DBTITLE 0,--i18n-106214eb-2fec-4a27-b692-035a86b8ec8d
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Extract Text Files as Raw Strings
# MAGIC
# MAGIC Working with text-based files, Used the **`text`** format to extract values from text fields.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM text.`${DA.paths.kafka_events}`

# COMMAND ----------

# DBTITLE 0,--i18n-732e648b-4274-48f4-86e9-8b42fd5a26bd
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Extract the Raw Bytes and Metadata of a File
# MAGIC
# MAGIC Used **`binaryFile`** to query unstructured data.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM binaryFile.`${DA.paths.kafka_events}`

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## When Direct Queries Don't Work 
# MAGIC
# MAGIC
# MAGIC CSV files are one of the most common file formats, but a direct query against these files rarely returns the desired results.
# MAGIC
# MAGIC ## Registering Tables on External Data with Read Options
# MAGIC
# MAGIC While Spark will extract some self-describing data sources efficiently using default settings, many formats will require declaration of schema or other options.
# MAGIC
# MAGIC The cell below demonstrates using Spark SQL DDL to create a table against an external CSV source, specifying:
# MAGIC 1. The column names and types
# MAGIC 1. The file format
# MAGIC 1. The delimiter used to separate fields
# MAGIC 1. The presence of a header
# MAGIC 1. The path to where this data is stored

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS sales_csv
# MAGIC   (order_id LONG, email STRING, transactions_timestamp LONG, total_item_quantity INTEGER, purchase_revenue_in_usd DOUBLE, unique_items INTEGER, items STRING)
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   header = "true",
# MAGIC   delimiter = "|"
# MAGIC )
# MAGIC LOCATION "${DA.paths.sales_csv}"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Wrapping with SQL:** To create a table against an external source in PySpark using the **`spark.sql()`** function.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS sales_csv
  (order_id LONG, email STRING, transactions_timestamp LONG, total_item_quantity INTEGER, purchase_revenue_in_usd DOUBLE, unique_items INTEGER, items STRING)
USING CSV
OPTIONS (
  header = "true",
  delimiter = "|"
)
LOCATION "{DA.paths.sales_csv}"
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sales_csv

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM sales_csv

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Running **DESCRIBE EXTENDED** on a table will show all of the metadata associated with the table definition.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED sales_csv

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Limits of Tables with External Data Sources
# MAGIC Note that while Delta Lake tables will guarantee that you always query the most recent version of your source data, tables registered against other data sources may represent older cached versions.

# COMMAND ----------

(spark.read
      .option("header", "true")
      .option("delimiter", "|")
      .csv(DA.paths.sales_csv)
      .write.mode("append")
      .format("csv")
      .save(DA.paths.sales_csv, header="true"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC If we look at the current count of records in our table, the number we see will not reflect these newly inserted rows.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM sales_csv

# COMMAND ----------

# MAGIC %md
# MAGIC We **can** manually refresh the cache of our data by running the **`REFRESH TABLE`** command.

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE sales_csv

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC Note that **refreshing** the table will invalidate our cache, meaning that we'll need to rescan the original data source and pull all data back into memory.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM sales_csv

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Extracting Data from SQL Databases
# MAGIC **syntax**
# MAGIC
# MAGIC <strong><code>
# MAGIC CREATE TABLE <jdbcTable><br/>
# MAGIC USING JDBC<br/>
# MAGIC OPTIONS (<br/>
# MAGIC &nbsp; &nbsp; url = "jdbc:{databaseServerType}://{jdbcHostname}:{jdbcPort}",<br/>
# MAGIC &nbsp; &nbsp; dbtable = "{jdbcDatabase}.table",<br/>
# MAGIC &nbsp; &nbsp; user = "{jdbcUsername}",<br/>
# MAGIC &nbsp; &nbsp; password = "{jdbcPassword}"<br/>
# MAGIC )
# MAGIC </code></strong>
# MAGIC
# MAGIC In the code sample below, I connected with <a href="https://www.sqlite.org/index.html" target="_blank">SQLite</a>.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS users_jdbc;
# MAGIC
# MAGIC CREATE TABLE users_jdbc
# MAGIC USING JDBC
# MAGIC OPTIONS (
# MAGIC   url = "jdbc:sqlite:${DA.paths.ecommerce_db}",
# MAGIC   dbtable = "users"
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC Now we can query this table as if it were defined locally.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED users_jdbc

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC Listing the contents of the specified location confirms that no data is being persisted locally.

# COMMAND ----------


import pyspark.sql.functions as F

location = spark.sql("DESCRIBE EXTENDED users_jdbc").filter(F.col("col_name") == "Location").first()["data_type"]
print(location)

files = dbutils.fs.ls(location)
print(f"Found {len(files)} files")

# COMMAND ----------

# DBTITLE 0,--i18n-9ac20d39-ae6a-400e-9e13-14af5d4c91df
# MAGIC %md
# MAGIC
# MAGIC To Delete the tables and files associated.

# COMMAND ----------

DA.cleanup()
