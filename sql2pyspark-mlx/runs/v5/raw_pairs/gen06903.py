from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("s.id") == F.col("o.store_id"), "left")
    .groupBy(F.col("s.id"), F.col("s.city"))
    .agg(
        F.count(F.col("o.id")).alias("order_count"),
        F.round(((100 * F.sum(F.when(F.col("o.status") == "CANCELLED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cancelled"),
        F.round(((100 * F.sum(F.when(F.col("o.channel") == "partner", 1).otherwise(0))) / F.count("*")), 1).alias("pct_partner"),
    )
)
