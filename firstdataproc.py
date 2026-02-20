from pyspark.sql import SparkSession
from pyspark.sql.functions import length

# ---------------------------------------
# 1. Initialize Spark Session
# ---------------------------------------
spark = SparkSession.builder \
    .appName("ComputeWordLength") \
    .getOrCreate()

# ---------------------------------------
# 2. Read from BigQuery
# ---------------------------------------
input_table = "myfirstcloudproject-487412.rawdataprocess.us_states_proc"

df = spark.read.format("bigquery") \
    .option("table", input_table) \
    .load()

# ---------------------------------------
# 3. Compute length of a column
#    Replace 'name' with your column name
# ---------------------------------------
result_df = df.withColumn("name_length", length(df["name"]))

# ---------------------------------------
# 4. Write result to BigQuery
# ---------------------------------------
output_table = "myfirstcloudproject-487412.rawdataprocess.us_states_length"

result_df.write.format("bigquery") \
    .option("table", output_table) \
    .mode("overwrite") \
    .save()
