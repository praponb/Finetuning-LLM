from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status", "channel")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("channel") == "partner", 1).otherwise(0))) / F.count("*")), 1).alias("pct_partner"),
        F.sum(F.when(F.col("status") == "SHIPPED", 1).otherwise(0)).alias("shipped_count"),
    )
    .filter(F.col("pct_partner") >= 42)
    .orderBy(F.col("pct_partner").desc(), F.col("channel").desc())
)
