from pyspark.sql import functions as F

filtered = (
    df_orders
    .filter((F.col("order_date") > F.to_date(F.lit("2023-07-22"))) & (F.col("amount") > 510))
    .withColumn("order_date_month_end", F.last_day("order_date"))
    .withColumn("channel_filled", F.coalesce("channel", F.lit("UNKNOWN")))
)

result = (
    filtered
    .groupBy("store_id")
    .pivot("status", ["COMPLETED", "CANCELLED", "SHIPPED"])
    .agg(F.max("shipping_fee"))
    .select(
        "store_id",
        F.col("COMPLETED").alias("completed"),
        F.col("CANCELLED").alias("cancelled"),
        F.col("SHIPPED").alias("shipped"),
    )
)
