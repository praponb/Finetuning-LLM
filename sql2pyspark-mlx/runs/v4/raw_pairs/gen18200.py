from pyspark.sql import functions as F

result = (
    df_orders
    .withColumn("amount_tier", F.when(F.col("amount") >= 1490, "PREMIUM").otherwise("STANDARD"))
    .groupingSets([["channel", "amount_tier"], ["channel"]], "channel", "amount_tier")
    .agg(
        F.percentile_approx("discount", 0.95).alias("discount_p95"),
        F.sum(F.when(F.col("channel") == "partner", F.col("amount")).otherwise(0)).alias("partner_amount"),
        F.round(F.sum("discount") / F.nullif(F.count(F.when(F.col("status") == "CANCELLED", 1)), F.lit(0)), 2).alias("discount_per_cancelled"),
        F.avg(F.when(F.col("channel") == "partner", F.col("discount"))).alias("partner_avg_discount"),
        F.countDistinct("status").alias("distinct_status"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("channel").desc_nulls_first())
)
