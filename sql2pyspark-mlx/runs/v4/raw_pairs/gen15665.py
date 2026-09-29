from pyspark.sql import functions as F

matched_keys = (
    df_subscriptions
    .select("customer_id", "status")
    .intersectAll(
        df_subscriptions
        .filter(~F.col("plan").isin(["premium", "standard"]))
        .select("customer_id", "status")
    )
)

kept_rows = (
    df_subscriptions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .withColumn("monthly_fee_tier", F.when(F.col("monthly_fee") >= 250, "PREMIUM").otherwise("STANDARD"))
    .cube("monthly_fee_tier")
    .agg(
        F.round(F.sum("monthly_fee") / F.nullif(F.count(F.when(F.col("status") == "ACTIVE", 1)), F.lit(0)), 2).alias("monthly_fee_per_active"),
        F.round(F.sum("monthly_fee") / F.nullif(F.count(F.when(F.col("status") == "EXPIRED", 1)), F.lit(0)), 2).alias("monthly_fee_per_expired"),
        F.sum("monthly_fee").alias("total_monthly_fee"),
        F.grouping_id().alias("gid"),
    )
)
