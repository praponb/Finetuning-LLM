from pyspark.sql import functions as F

base = (
    df_orders
    .filter((F.col("order_date") > F.lit("2024-02-26").cast("date")) & F.col("channel").isNotNull())
    .select("*", F.length("status").alias("status_length"))
)

result = (
    base
    .groupBy("store_id")
    .pivot("status", ["RETURNED", "PENDING", "CANCELLED"])
    .agg(F.avg("shipping_fee").alias("total"), F.count(F.lit(1)).alias("item_count"))
)
