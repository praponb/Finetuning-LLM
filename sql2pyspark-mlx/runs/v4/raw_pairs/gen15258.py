from pyspark.sql import functions as F

valid_shipments = (
    df_shipments
    .filter((F.col("status") == "IN_TRANSIT") & F.col("order_id").isNotNull())
    .withColumn("status_carrier_key", F.concat_ws("-", "status", "carrier"))
)

agg = (
    valid_shipments
    .groupBy("status")
    .agg(F.min("cost").alias("min_cost"))
)

result = agg.filter(F.col("min_cost") >= 71)
