from pyspark.sql.functions import *
from pyspark.sql.types import *

#ADLS Configuration
spark.conf.set(
  "fs.azure.account.key.<<StorageAccount_Name>>.dfs.core.windows.net",
  "<<StorageAccount_Access_key>>"
)

bronze_path = "abfss://<<container>>@<<StorageAccount_Name>>.dfs.core.windows.net/<<path>>"
silver_path = "abfss://<<container>>@<<StorageAccount_Name>>.dfs.core.windows.net/<<path>>"

#read from bronze
bronze_df = (
    spark.readStream
    .format("delta")
    .load(bronze_path)
)

#define schema
schema = StructType([
    StructField("patient_id", StringType()),
    StructField("gender", StringType()),
    StructField("age", IntegerType()),
    StructField("department", StringType()),
    StructField("admission_time", StringType()),
    StructField("discharge_time", StringType()),
    StructField("bed_id", IntegerType()),
    StructField("hospital_id", IntegerType())
    ])

#pparse dataframe
parsed_df = bronze_df.withColumn("data", from_json(col("raw_json"), schema)).select("data.*")

#conevrt type into timestamp
clean_df = parsed_df.withColumn("admission_time", to_timestamp("admission_time"))
clean_df = clean_df.withColumn("discharge_time", to_timestamp("discharge_time"))

#invalid admission time
clean_df = clean_df.withColumn("admission_time", 
                               when(
                                 col("admission_time").isNull() | (col("admission_time") > current_timestamp()),
                                 current_timestamp())
                               .otherwise(col("admission_time"))
                               )

#Handle Invalid age
clean_df = clean_df.withColumn("age", 
                               when(
                                 col("age") > 100, floor(rand()*90+1).cast("int"))
                               .otherwise(col("age")))

#schema evolution
expected_cols = ["patient_id", "gender", "age", "department", "admission_time", "discharge_time", "bed_id", "hospital_id"]

for col_name in expected_cols:
  if col_name not in clean_df.columns:
    clean_df = clean_df.withColumn(col_name, lit(None))

#write to silver
(
  clean_df.writeStream
  .format("delta")
  .outputMode("append")
  .option("mergeSchema", "true")
  .option("checkpointLocation", silver_path + "_checkpoint")
  .start(silver_path)
)


display(spark.read.format("delta").load(silver_path))