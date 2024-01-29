# Databricks notebook source
# MAGIC %md
# MAGIC This notebook provides examples of two different ways to load table metadata into Unity Catalog. 
# MAGIC * The first has the data dictionary loaded into its own table, and then populates the metadata of the target tables with that information.
# MAGIC * The second approach shows loading from data dictionaries that are in delimited text files stored in a volume, and the metadata is taken from the text files and loaded into the target tables. This example also shows loading table descriptions from a separate data dictionary.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Option 1: Data Dictionary Table to Target Tables
# Load the data from the data dictionary table into dataframe
# The structure of the data dictionary should be
# table_name | column_name | column_description

dict_table="your_catalog.your_schema.your_data_dictionary_table" # UPDATE with your data dictionary table info 
df_data_dict = spark.sql(f"SELECT * FROM {dict_table}")

# Write the data from the data dictionary into the columns of the target table
catalog = "your_target_catalog" # UPDATE with your catalog where your target tables are located
schema = "your_target_schema" # UPDATE with your schema where your target tables are located
for row in df_data_dict.collect():
    table = row[0]
    col = row[1]
    desc = row[2].replace("'", "\\'")
    try:# If a column is in the dict but not in catalog, the sql statement will fail. The 'try' statement will allow the loop to continue regardless.
        spark.sql(f"ALTER TABLE {catalog}.{schema}.{table} ALTER COLUMN {col} COMMENT '{desc}'")
    except:
        pass


# COMMAND ----------

# DBTITLE 1,Option 2: Data Dictionary Text Files to Target Tables
# Load data from the data dictionary text files into dataframes.
# The structure of the text file for the table dictionary should be
# «TableName«|«Description«
# The structure for the text file for the column dictionary should be
# «TableName«|«ColumnName«|«Description«

table_dict_path = "/Volumes/your_volume_path/TableMeta.txt" # UPDATE with the path to the text file with your table descriptions
column_dict_path = "/Volumes/your_volume_path/ColumnMeta.txt" # UPDATE with the path to the text file with your column descriptions

table_dict_df = spark.read \
                .option("header", "true") \
                .option("delimiter", "|") \
                .option("inferSchema", "true") \
                .option("quoteAll","true")\
                .option("quote", "�")\
                .option("multiline", "true")\
                .csv(table_dict_path)

column_dict_df = spark.read \
                .option("header", "true") \
                .option("delimiter", "|") \
                .option("inferSchema", "true") \
                .option("quoteAll","true")\
                .option("quote", "�")\
                .option("multiline", "true")\
                .csv(column_dict_path)


# COMMAND ----------

# DBTITLE 1,Write descriptions to tables and columns from dictionaries
# Write the data from the data dictionary into the comments of the target 
catalog = "your_target_catalog" # UPDATE with the name of the catalog where the target tables are located
schema = "your_target_schema" # UPDATE with the name of the schema where the target tables are located

for row in column_dict_df.collect():
    table = row[0]
    col = row[1]
    desc = row[2].replace("'", "\\'")
    try: # If a column is in the dict but not in catalog, the sql statement will fail. The 'try' statement will allow the loop to continue regardless.
        spark.sql(f"ALTER TABLE {catalog}.{schema}.{table} ALTER COLUMN {col} COMMENT '{desc}'")
    except:
        pass

# update the table comments 
for row in table_dict_df.collect():
    table = row[0]
    desc = row[1].replace("'", "\\'")
    try: # If a table is in the dict but not in catalog, the sql statement will fail. The 'try' statement will allow the loop to continue regardless.
        spark.sql(f"ALTER TABLE {catalog}.{schema}.{table} SET TBLPROPERTIES ('comment' = '{desc}')")
    except:
        pass
