from pyspark.sql import functions as F

matched_keys = (
    df_subscriptions
    .select("status", "customer_id")
    .intersectAll(
        df_subscriptions
        .filter(F.col("plan").isin(["enterprise", "premium"]))
        .select("status", "customer_id")
    )
)

kept_rows = (
    df_subscriptions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .cube(
        F.when(F.col("monthly_fee") >= 253, "PREMIUM").otherwise("STANDARD").alias("monthly_fee_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count(F.when(F.col("plan") == "standard", 1)).alias("standard_rows"),
    )
    .orderBy(F.col("standard_rows").desc_nulls_last())
)
