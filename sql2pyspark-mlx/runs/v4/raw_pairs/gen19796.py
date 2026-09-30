from pyspark.sql import functions as F

src = df_orders.filter(F.col("discount").isNotNull())

summary = (
    src
    .groupBy(
        F.when(F.col("channel").isin(["store", "mobile"]), "priority").otherwise("other").alias("channel_group"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "PENDING", 1).otherwise(0))) / F.count("*")), 1).alias("pct_pending"),
        F.sum(F.when(F.col("channel") == "web", 1).otherwise(0)).alias("web_count"),
    )
)

result = (
    summary
    .select("channel_group", "pct_pending")
    .orderBy(F.col("pct_pending").desc())
)
