-- Databricks notebook source
-- DBTITLE 0,--i18n-2ad42144-605b-486f-ad65-ca24b47b1924
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC # Data Cleaning and Transformations
-- MAGIC
-- MAGIC ## Learning Outcomes
-- MAGIC - Summarized datasets and describe null behaviors
-- MAGIC - Retrieved and remove duplicates
-- MAGIC - Validated datasets for expected counts, missing values, and duplicate records
-- MAGIC - Applied common transformations to clean and transform data.
-- MAGIC - Used **`.`** and **`:`** syntax to query nested data
-- MAGIC - Parsed JSON strings into structs
-- MAGIC - Flatten and unpack arrays and structs
-- MAGIC - Combined datasets using joins
-- MAGIC - Reshaped data using pivot tables

-- COMMAND ----------

-- MAGIC %run ./Includes/file-Setup-02.4

-- COMMAND ----------

-- DBTITLE 0,--i18n-31202e20-c326-4fa0-8892-ab9308b4b6f0
-- MAGIC %md
-- MAGIC ## Data Overview
-- MAGIC
-- MAGIC | field | type | description |
-- MAGIC |---|---|---|
-- MAGIC | user_id | string | unique identifier |
-- MAGIC | user_first_touch_timestamp | long | time at which the user record was created in microseconds since epoch |
-- MAGIC | email | string | most recent email address provided by the user to complete an action |
-- MAGIC | updated | timestamp | time at which this record was last updated |
-- MAGIC

-- COMMAND ----------


SELECT count(*), count(user_id), count(user_first_touch_timestamp), count(email), count(updated)
FROM users_dirty

-- COMMAND ----------

-- DBTITLE 0,--i18n-c414c24e-3b72-474b-810d-c3df32032c26
-- MAGIC %md
-- MAGIC
-- MAGIC ## Inspect Missing Data

-- COMMAND ----------


SELECT count_if(email IS NULL) FROM users_dirty;
SELECT count(*) FROM users_dirty WHERE email IS NULL;

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC from pyspark.sql.functions import col
-- MAGIC usersDF = spark.read.table("users_dirty")
-- MAGIC
-- MAGIC usersDF.selectExpr("count_if(email IS NULL)")
-- MAGIC usersDF.where(col("email").isNull()).count()

-- COMMAND ----------

-- DBTITLE 0,--i18n-ea1ca35c-6421-472b-b70b-4f36bdab6d79
-- MAGIC %md
-- MAGIC  
-- MAGIC ## Deduplicate Rows
-- MAGIC **`DISTINCT *`** to remove true duplicate records where entire rows contain the same values.

-- COMMAND ----------


SELECT DISTINCT(*) FROM users_dirty

-- COMMAND ----------

-- MAGIC %python
-- MAGIC usersDF.distinct().display()

-- COMMAND ----------

-- DBTITLE 0,--i18n-5da6599b-756c-4d22-85cd-114ff02fc19d
-- MAGIC %md
-- MAGIC
-- MAGIC   
-- MAGIC ## Deduplicate Rows Based on Specific Columns
-- MAGIC
-- MAGIC The code below uses **`GROUP BY`** to remove duplicate records based on **`user_id`** and **`user_first_touch_timestamp`** column values.

-- COMMAND ----------


CREATE OR REPLACE TEMP VIEW deduped_users AS 
SELECT user_id, user_first_touch_timestamp, max(email) AS email, max(updated) AS updated
FROM users_dirty
WHERE user_id IS NOT NULL
GROUP BY user_id, user_first_touch_timestamp;

SELECT count(*) FROM deduped_users

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import max
-- MAGIC dedupedDF = (usersDF
-- MAGIC     .where(col("user_id").isNotNull())
-- MAGIC     .groupBy("user_id", "user_first_touch_timestamp")
-- MAGIC     .agg(max("email").alias("email"), 
-- MAGIC          max("updated").alias("updated"))
-- MAGIC     )
-- MAGIC
-- MAGIC dedupedDF.count()

-- COMMAND ----------


SELECT COUNT(DISTINCT(user_id, user_first_touch_timestamp))
FROM users_dirty
WHERE user_id IS NOT NULL

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (usersDF
-- MAGIC     .dropDuplicates(["user_id", "user_first_touch_timestamp"])
-- MAGIC     .filter(col("user_id").isNotNull())
-- MAGIC     .count())

-- COMMAND ----------

-- DBTITLE 0,--i18n-776b4ee7-9f29-4a19-89da-1872a1f8cafa
-- MAGIC %md
-- MAGIC
-- MAGIC ## Validate Datasets
-- MAGIC  
-- MAGIC perform validation using simple filters and **`WHERE`** clauses.
-- MAGIC
-- MAGIC Validate that the **`user_id`** for each row is unique.

-- COMMAND ----------


SELECT max(row_count) <= 1 no_duplicate_ids FROM (
  SELECT user_id, count(*) AS row_count
  FROM deduped_users
  GROUP BY user_id)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import count
-- MAGIC
-- MAGIC display(dedupedDF
-- MAGIC     .groupBy("user_id")
-- MAGIC     .agg(count("*").alias("row_count"))
-- MAGIC     .select((max("row_count") <= 1).alias("no_duplicate_ids")))

-- COMMAND ----------

-- DBTITLE 0,--i18n-d405e7cd-9add-44e3-976a-e56b8cdf9d83
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC Confirm that each email is associated with at most one **`user_id`**.

-- COMMAND ----------

SELECT max(user_id_count) <= 1 at_most_one_id FROM (
  SELECT email, count(user_id) AS user_id_count
  FROM deduped_users
  WHERE email IS NOT NULL
  GROUP BY email)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC display(dedupedDF
-- MAGIC     .where(col("email").isNotNull())
-- MAGIC     .groupby("email")
-- MAGIC     .agg(count("user_id").alias("user_id_count"))
-- MAGIC     .select((max("user_id_count") <= 1).alias("at_most_one_id")))

-- COMMAND ----------

-- DBTITLE 0,--i18n-8630c04d-0752-404f-bfd1-bb96f7b06ffa
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC ## Date Format and Regex
-- MAGIC Now that we've removed null fields and eliminated duplicates, we may wish to extract further value out of the data.
-- MAGIC
-- MAGIC The code below:
-- MAGIC - Casts the **`user_first_touch_timestamp`** to a valid timestamp
-- MAGIC - Extracts the calendar date and clock time for this timestamp in human readable format
-- MAGIC - Uses **`regexp_extract`** to extract the domains from the email column using regex

-- COMMAND ----------

SELECT *, 
  date_format(first_touch, "MMM d, yyyy") AS first_touch_date,
  date_format(first_touch, "HH:mm:ss") AS first_touch_time,
  regexp_extract(email, "(?<=@).+", 0) AS email_domain
FROM (
  SELECT *,
    CAST(user_first_touch_timestamp / 1e6 AS timestamp) AS first_touch 
  FROM deduped_users
)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import date_format, regexp_extract
-- MAGIC
-- MAGIC display(dedupedDF
-- MAGIC     .withColumn("first_touch", (col("user_first_touch_timestamp") / 1e6).cast("timestamp"))
-- MAGIC     .withColumn("first_touch_date", date_format("first_touch", "MMM d, yyyy"))
-- MAGIC     .withColumn("first_touch_time", date_format("first_touch", "HH:mm:ss"))
-- MAGIC     .withColumn("email_domain", regexp_extract("email", "(?<=@).+", 0))
-- MAGIC )

-- COMMAND ----------

-- DBTITLE 0,--i18n-c9e02918-f105-4c12-b553-3897fa7387cc
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC Run the following cell to delete the tables and files associated with this lesson.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## COMPLEX TRANSFORMATIONS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Data Overview
-- MAGIC
-- MAGIC The **`events_raw`** table was registered against data representing a Kafka payload.
-- MAGIC
-- MAGIC cast the **`key`** and **`value`** as strings to view these values in a human-readable format.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW events_strings AS 
SELECT string(key), string(value) FROM events_raw;

SELECT * FROM events_strings

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col
-- MAGIC
-- MAGIC events_stringsDF = (spark
-- MAGIC     .table("events_raw")
-- MAGIC     .select(col("key").cast("string"), 
-- MAGIC             col("value").cast("string"))
-- MAGIC     )
-- MAGIC display(events_stringsDF)

-- COMMAND ----------

-- DBTITLE 0,--i18n-67712d1a-cae1-41dc-8f7b-cc97e933128e
-- MAGIC %md
-- MAGIC ## Manipulate Complex Types

-- COMMAND ----------

-- DBTITLE 0,--i18n-c6a0cd9e-3bdc-463a-879a-5551fa9a8449
-- MAGIC %md
-- MAGIC
-- MAGIC ### Work with Nested Data
-- MAGIC
-- MAGIC The code cell below queries the converted strings to view an example JSON object without null fields.
-- MAGIC
-- MAGIC - Use **`:`** syntax in queries to access subfields in JSON strings
-- MAGIC - Use **`.`** syntax in queries to access subfields in struct types

-- COMMAND ----------


SELECT * FROM events_strings WHERE value:event_name = "finalize" ORDER BY key LIMIT 1

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(events_stringsDF
-- MAGIC     .where("value:event_name = 'finalize'")
-- MAGIC     .orderBy("key")
-- MAGIC     .limit(1)
-- MAGIC )

-- COMMAND ----------

-- DBTITLE 0,--i18n-914b04cd-a1c1-4a91-aea3-ecd87714ea7d
-- MAGIC %md
-- MAGIC Let's use the JSON string example above to derive the schema, then parse the entire JSON column into struct types.
-- MAGIC - **`schema_of_json()`** returns the schema derived from an example JSON string.
-- MAGIC - **`from_json()`** parses a column containing a JSON string into a struct type using the specified schema.
-- MAGIC
-- MAGIC After we unpack the JSON string to a struct type, let's unpack and flatten all struct fields into columns.
-- MAGIC - **`*`** unpacking can be used to flattens structs; **`col_name.*`** pulls out the subfields of **`col_name`** into their own columns.

-- COMMAND ----------


SELECT schema_of_json('{"device":"Linux","ecommerce":{"purchase_revenue_in_usd":1075.5,"total_item_quantity":1,"unique_items":1},"event_name":"finalize","event_previous_timestamp":1593879231210816,"event_timestamp":1593879335779563,"geo":{"city":"Houston","state":"TX"},"items":[{"coupon":"NEWBED10","item_id":"M_STAN_K","item_name":"Standard King Mattress","item_revenue_in_usd":1075.5,"price_in_usd":1195.0,"quantity":1}],"traffic_source":"email","user_first_touch_timestamp":1593454417513109,"user_id":"UA000000106116176"}') AS schema

-- COMMAND ----------


CREATE OR REPLACE TEMP VIEW parsed_events AS SELECT json.* FROM (
SELECT from_json(value, 'STRUCT<device: STRING, ecommerce: STRUCT<purchase_revenue_in_usd: DOUBLE, total_item_quantity: BIGINT, unique_items: BIGINT>, event_name: STRING, event_previous_timestamp: BIGINT, event_timestamp: BIGINT, geo: STRUCT<city: STRING, state: STRING>, items: ARRAY<STRUCT<coupon: STRING, item_id: STRING, item_name: STRING, item_revenue_in_usd: DOUBLE, price_in_usd: DOUBLE, quantity: BIGINT>>, traffic_source: STRING, user_first_touch_timestamp: BIGINT, user_id: STRING>') AS json 
FROM events_strings);

SELECT * FROM parsed_events

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import from_json, schema_of_json
-- MAGIC
-- MAGIC json_string = """
-- MAGIC {"device":"Linux","ecommerce":{"purchase_revenue_in_usd":1047.6,"total_item_quantity":2,"unique_items":2},"event_name":"finalize","event_previous_timestamp":1593879787820475,"event_timestamp":1593879948830076,"geo":{"city":"Huntington Park","state":"CA"},"items":[{"coupon":"NEWBED10","item_id":"M_STAN_Q","item_name":"Standard Queen Mattress","item_revenue_in_usd":940.5,"price_in_usd":1045.0,"quantity":1},{"coupon":"NEWBED10","item_id":"P_DOWN_S","item_name":"Standard Down Pillow","item_revenue_in_usd":107.10000000000001,"price_in_usd":119.0,"quantity":1}],"traffic_source":"email","user_first_touch_timestamp":1593583891412316,"user_id":"UA000000106459577"}
-- MAGIC """
-- MAGIC parsed_eventsDF = (events_stringsDF
-- MAGIC     .select(from_json("value", schema_of_json(json_string)).alias("json"))
-- MAGIC     .select("json.*")
-- MAGIC )
-- MAGIC
-- MAGIC display(parsed_eventsDF)

-- COMMAND ----------

-- DBTITLE 0,--i18n-5ca54e9c-dcb7-4177-99ab-77377ce8d899
-- MAGIC %md
-- MAGIC ### Manipulate Arrays

-- COMMAND ----------


CREATE OR REPLACE TEMP VIEW exploded_events AS
SELECT *, explode(items) AS item
FROM parsed_events;

SELECT * FROM exploded_events WHERE size(items) > 2

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import explode, size
-- MAGIC
-- MAGIC exploded_eventsDF = (parsed_eventsDF
-- MAGIC     .withColumn("item", explode("items"))
-- MAGIC )
-- MAGIC
-- MAGIC display(exploded_eventsDF.where(size("items") > 2))

-- COMMAND ----------

DESCRIBE exploded_events

-- COMMAND ----------

-- DBTITLE 0,--i18n-0810444d-1ce9-4cb7-9ba9-f4596e84d895
-- MAGIC %md
-- MAGIC The code below combines array transformations to create a table that shows the unique collection of actions and the items in a user's cart.
-- MAGIC - **`collect_set()`** collects unique values for a field, including fields within arrays.
-- MAGIC - **`flatten()`** combines multiple arrays into a single array.
-- MAGIC - **`array_distinct()`** removes duplicate elements from an array.

-- COMMAND ----------


SELECT user_id,
  collect_set(event_name) AS event_history,
  array_distinct(flatten(collect_set(items.item_id))) AS cart_history
FROM exploded_events
GROUP BY user_id

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC from pyspark.sql.functions import array_distinct, collect_set, flatten
-- MAGIC
-- MAGIC display(exploded_eventsDF
-- MAGIC     .groupby("user_id")
-- MAGIC     .agg(collect_set("event_name").alias("event_history"),
-- MAGIC             array_distinct(flatten(collect_set("items.item_id"))).alias("cart_history"))
-- MAGIC )

-- COMMAND ----------

-- DBTITLE 0,--i18n-8744b315-393b-4f8b-a8c1-3d6f9efa93b0
-- MAGIC %md
-- MAGIC  
-- MAGIC ## Combine and Reshape Data

-- COMMAND ----------

-- DBTITLE 0,--i18n-15407508-ba1c-4aef-bd40-1c8eb244ed83
-- MAGIC %md
-- MAGIC  
-- MAGIC ### Join Tables

-- COMMAND ----------


CREATE OR REPLACE TEMP VIEW item_purchases AS

SELECT * 
FROM (SELECT *, explode(items) AS item FROM sales) a
INNER JOIN item_lookup b
ON a.item.item_id = b.item_id;

SELECT * FROM item_purchases

-- COMMAND ----------

-- MAGIC %python
-- MAGIC exploded_salesDF = (spark
-- MAGIC     .table("sales")
-- MAGIC     .withColumn("item", explode("items"))
-- MAGIC )
-- MAGIC
-- MAGIC itemsDF = spark.table("item_lookup")
-- MAGIC
-- MAGIC item_purchasesDF = (exploded_salesDF
-- MAGIC     .join(itemsDF, exploded_salesDF.item.item_id == itemsDF.item_id)
-- MAGIC )
-- MAGIC
-- MAGIC display(item_purchasesDF)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Pivot Tables
-- MAGIC The following code cell uses **`PIVOT`** to flatten out the item purchase information contained in several fields derived from the **`sales`** dataset. This flattened data format can be useful for dashboarding, but also useful for applying machine learning algorithms for inference or prediction.

-- COMMAND ----------


SELECT *
FROM item_purchases
PIVOT (
  sum(item.quantity) FOR item_id IN (
    'P_FOAM_K',
    'M_STAN_Q',
    'P_FOAM_S',
    'M_PREM_Q',
    'M_STAN_F',
    'M_STAN_T',
    'M_PREM_K',
    'M_PREM_F',
    'M_STAN_K',
    'M_PREM_T',
    'P_DOWN_S',
    'P_DOWN_K')
)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC transactionsDF = (item_purchasesDF
-- MAGIC     .groupBy("order_id", 
-- MAGIC         "email",
-- MAGIC         "transaction_timestamp", 
-- MAGIC         "total_item_quantity", 
-- MAGIC         "purchase_revenue_in_usd", 
-- MAGIC         "unique_items",
-- MAGIC         "items",
-- MAGIC         "item",
-- MAGIC         "name",
-- MAGIC         "price")
-- MAGIC     .pivot("item_id")
-- MAGIC     .sum("item.quantity")
-- MAGIC )
-- MAGIC display(transactionsDF)

-- COMMAND ----------

-- DBTITLE 0,--i18n-b89c0c3e-2352-4a82-973d-7e655276bede
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC Run the following cell to delete the tables and files associated with this lesson.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()
