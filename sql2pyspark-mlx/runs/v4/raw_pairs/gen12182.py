from pyspark.sql import functions as F

cleaned = (
    df_orders
    .filter((F.col("order_date") > F.lit("2023-06-01").cast("date")) & F.col("channel").isNotNull() & (F.col("store_id") == 16))
    .select(
        "*",
        F.date_trunc("week", "order_date").alias("order_date_period"),
        F.coalesce("channel", F.lit("DIRECT")).alias("resolved_channel"),
    )
)

result = (
    cleaned
    .groupBy("channel")
    .pivot("status", ["COMPLETED", "PENDING"])
    .agg(F.avg("amount").alias("value_sum"))
)
