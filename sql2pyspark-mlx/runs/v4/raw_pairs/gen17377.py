from pyspark.sql import functions as F

cleaned = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-06-14").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "*",
        F.when(F.col("status").isin(["COMPLETED", "RETURNED"]), "Tier 1").otherwise("Other").alias("status_group"),
    )
)

result = (
    cleaned
    .groupBy("store_id")
    .pivot("channel", ["store", "web"])
    .agg(F.avg("shipping_fee").alias("value_sum"), F.count(F.lit(1)).alias("cnt"))
)
