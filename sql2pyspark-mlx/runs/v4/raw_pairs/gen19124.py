from pyspark.sql import functions as F

kept_keys = (
    df_orders
    .select("status", "channel")
    .subtract(
        df_orders
        .filter(F.col("discount") <= 0.1)
        .select("status", "channel")
    )
)

reconciled = (
    df_orders
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .withColumn("discount_tier", F.when(F.col("discount") >= 0.38, "LARGE").otherwise("SMALL"))
    .withColumn("activity_month", F.date_trunc("month", "order_date"))
    .rollup("discount_tier", "activity_month")
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("channel") == "partner", 1)), F.lit(0)), 2).alias("amount_per_partner"),
        F.count_if(F.col("status") == "SHIPPED").alias("shipped_events"),
        F.min("discount").alias("min_discount"),
        F.grouping_id().alias("gid"),
    )
    .orderBy("cnt")
)
