## Data-Engineering-with-Databricks (My Personel DA handbook/Practice Material)

## I practised the core Databricks funtionalities :

## 1. Transformation of Data With Spark, (Notebooks 1 -3 )
## 2. Managed Data with Delta Lake, (Notebooks 4 and 5)
## 3. Built Data Pipelines with Delta Live Tables,
## 4. Managed Data Access with Unity Catalog.

## Notebook 1: Data Extraction
Worked with a sample of raw Kafka JSON files. 
## My Learning Outcomes
- Used Spark SQL to directly query data files.
- Create tables against external data sources for various file formats.
- Describe default behavior when querying tables defined against external sources.
- Applied Layer views and CTEs to make referencing data files easier.
- Leveraged **`text`** and **`binaryFile`** methods to review raw file contents.

## Contents
 - Query a Single File.
 - Query a Directory of Files.
 - Create References to Files.
 - Create Temporary References to Files.
 - CTEs for Reference within a Query.
 - Extract Text Files as Raw Strings.
 - Extract the Raw Bytes and Metadata of a File.
 - Registering Tables on External Data with Read Options.
 - Limits of Tables with External Data Sources.
 - Extracting Data from SQL Databases.

## Notebook 2: Data Cleaning and Transformations
## Learning Outcomes
- Summarized datasets and described null behaviors
- Retrieved and removed duplicates
- Validated datasets for expected counts, missing values, and duplicate records
- Applied common transformations to clean and transform data
- Used **`.`** and **`:`** syntax to query nested data
- Parsed JSON strings into structs
- Flatten and unpack arrays and structs
- Combined datasets using joins
- Reshaped data using pivot tables

## Contents
- Inspect Missing Data
- Deduplicate Rows
- Deduplicate Rows Based on Specific Columns
- Validate Datasets
- Date Format and Regex
- Complex Tranformations : Manipulate complex type (nested data), Manipulate Array, Combine and Reshape Data (Join, pivot tables)

## Notebook 3 - SQL UDF's
## Learning Outcomes
- Defined and registering SQL UDFs- Described the security model used for sharing SQL UDFs
- Used **`CASE`** / **`WHEN`** statements in SQL code
- Leveraged **`CASE`** / **`WHEN`** statements in SQL UDFs for custom control flow

## Contents
- User Defined Function Def
- Scoping and Permissions of SQL UDFs
- Control Flow Functions.

## Notebook 4 - Schemas, Tables on Databricks, Setting up Delta Table, Load Data
## Learning Outcomes
- Used Spark SQL DDL to define schemas and tables
- Described how the **`LOCATION`** keyword impacts the default storage directory
- Used CTAS statements to create Delta Lake tables
- Created new tables from existing views or tables
- Enriched loaded data with additional metadata
- Declared table schema with generated columns and descriptive comments
- Set advanced options to control data location, quality enforcement, and partitioning
- Created **shallow and deep clones**
- Overwrite data tables using **`INSERT OVERWRITE`**
- Append to a table using **`INSERT INTO`**
- Append, update, and delete from a table using **`MERGE INTO`**
- Ingest data incrementally into tables using **`COPY INTO`**

## Contents
- Schemas
- Managed Tables, External Tables
- Setting Up Delta Tables
- Create Table as Select (CTAS)
- Filtering and Renaming Columns from Existing Tables
- Declare Schema with Generated Columns
- Add a Table Constraint
- Enrich Tables with Additional Options and Metadata
- Cloning Delta Lake Tables
- Append Rows, Merge Updates, Insert-Only Merge for Deduplication, Load Incrementally

## Notebook 5 - Versioning, Vaccuming, and Optimization

## Learning Outcome:
* Used **`OPTIMIZE`** to compact small files
* Used **`ZORDER`** to index tables
* Described the directory structure of Delta Lake files
* Reviewed a history of table transactions
* Queried and roll back to previous table version
* Clean up stale data files with **`VACUUM`**
