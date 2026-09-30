from pyspark.sql import functions as F

base = (
    df_orders
    .filter(F.col("order_date") > F.lit("2023-04-13").cast("date"))
    .select(
        "*",
        F.coalesce("discount", F.lit(0)).alias("discount_filled"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_filled"),
    )
)

result = (
    base
    .groupBy("status")
    .pivot("channel", ["mobile", "store", "partner"])
    .agg(F.max("discount_filled").alias("amount"), F.count(F.lit(1)).alias("n"))
)
