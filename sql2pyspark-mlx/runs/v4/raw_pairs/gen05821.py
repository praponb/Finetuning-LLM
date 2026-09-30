from pyspark.sql import functions as F

result = (
    df_shipments
    .withColumn("status_carrier_key", F.concat_ws("-", "status", "carrier"))
    .withColumn("is_flash", F.when(F.col("carrier") == "Flash", 1).otherwise(0))
    .orderBy(F.col("is_flash").desc(), "delivered_date")
)
