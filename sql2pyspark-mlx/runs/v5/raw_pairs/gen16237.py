from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id") == 24)
    .withColumn("status_prefix", F.substring("status", 1, 3))
    .withColumn("clean_status", F.trim("status"))
)
