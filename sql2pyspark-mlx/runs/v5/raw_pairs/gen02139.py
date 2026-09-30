from pyspark.sql import functions as F

recent_orders = df_orders.filter(F.col("amount").between(550, 1120))

channel_summary = (
    recent_orders
    .groupBy(
        "channel",
        F.when(F.col("discount") >= 0.41, "high").when(F.col("discount") >= 0.08, "mid").otherwise("low").alias("discount_bucket"),
    )
    .agg(
        F.min("discount").alias("min_discount"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
)

result = (
    channel_summary
    .filter(F.col("amount_range") > 31)
    .select("channel", "discount_bucket", "min_discount", "amount_range")
    .orderBy(F.col("amount_range").desc())
)
