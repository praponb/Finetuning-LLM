from pyspark.sql import functions as F

cleaned = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2024-04-10").cast("date")) & (F.col("order_id") == 39))
    .withColumn("status_group", F.when(F.col("status").isin(["SUCCESS", "FAILED"]), "Group A").otherwise("Other"))
    .withColumn("method_clean", F.coalesce("method", F.lit("UNKNOWN")))
)

result = (
    cleaned
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .select("s.*", F.col("o.channel"), F.col("o.status").alias("order_status"))
)
