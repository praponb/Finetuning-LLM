from pyspark.sql import functions as F

valid_orders = (
    df_orders
    .filter(F.col("store_id").isNull() & (F.col("discount") < 0.12))
    .withColumn("status_channel", F.concat("status", F.lit(", "), "channel"))
)

status_summary = (
    valid_orders
    .groupBy("status")
    .agg(F.count("*").alias("num_orders"))
)

result = status_summary.select("status", "num_orders")
