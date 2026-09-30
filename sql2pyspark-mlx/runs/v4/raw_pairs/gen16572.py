from pyspark.sql import functions as F

src = (
    df_orders
    .filter((F.col("channel") == "mobile") & (F.col("amount").isNotNull() | F.col("customer_id").isin([2, 16, 40])))
)

orders_stats = (
    src
    .groupBy("status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("channel") == "web", 1).otherwise(0))) / F.count("*")), 1).alias("pct_web"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
)

result = (
    orders_stats
    .select("status", "pct_web", "avg_amount")
    .orderBy("status")
)
