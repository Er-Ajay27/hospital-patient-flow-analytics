from pyspark.sql.functions import *

#Azure Event Hub Configuration
event_hub_namespace = "<<namespace hostname>>"
event_hub_name = "<<Event_hub_name>>"
event_hub_conn_str = "<<Connection String>>

kafka_options = {
    'kafka.bootstrap.servers': f"{event_hub_namespace}:9093",
    'subscribe': event_hub_name,
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'PLAIN',
    'kafka.sasl.jaas.config': f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{event_hub_conn_str}";',
    'startingOffsets': 'latest',
    'failOnDataLoss': 'false'
}

#Read from Eventhub
raw_df = (spark.readStream
          .format('kafka')
          .options(**kafka_options)
          .load()
          )

#Cast data to json
json_df = raw_df.selectExpr("CAST(value AS STRING) as raw_json")

#ADLS Configuration
spark.conf.set(
  "fs.azure.account.key.<<StorageAccount_Name>>.dfs.core.windows.net",
  "<<StorageAccount_Access_key>>"
)

bronze_path = "abfss://<<container>>@<<StorageAccount_Name>>.dfs.core.windows.net/<<path>>"

checkpoint_path = "abfss://<<container>>@<<StorageAccount_Name>>.dfs.core.windows.net/_checkpoints/<<path>>"

#Write stream to bronze
(
  json_df
  .writeStream
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", checkpoint_path)
  .start(bronze_path)
)



display(spark.read.format("delta").load(bronze_path).count())

------------------------------------------------------------------


manual_json = '{"patient_id": "74ffa221-92f2-4b4c-b3df-0094b1382c03", "gender": "Male", "age": 60, "department": "Emergency", "admission_time": "2026-05-06T15:26:41.646641", "discharge_time": "2026-05-07T05:26:41.646641", "bed_id": 78, "hospital_id": 7}'

manual_df = spark.createDataFrame([(manual_json)],["raw_json"])

#Write stream to bronze
(
  manual_df
  .write
  .format("delta")
  .mode("append")
  .save(bronze_path)
)
