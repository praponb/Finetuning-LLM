from pyspark.sql import functions as F

matched_keys = (
    df_customers
    .select("id", "segment")
    .intersectAll(
        df_customers
        .filter(F.col("city") == "Tokyo")
        .select("id", "segment")
    )
)

reconciled = (
    df_customers
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.segment") == F.col("k.segment")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.date_trunc("WEEK", "signup_date").alias("week_start"),
        F.when(F.col("age") >= 63, "HIGH").otherwise("LOW").alias("age_tier"),
    )
    .agg(
        F.stddev_samp("credit_limit").alias("credit_limit_stddev"),
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_rows"),
        F.sum(F.when(F.col("country") == "SG", F.col("age")).otherwise(0)).alias("sg_age"),
        F.round(F.sum("age") / F.nullif(F.count(F.when(F.col("city") == "Chiang Mai", 1)), F.lit(0)), 2).alias("age_per_chiang_mai"),
    )
    .orderBy(F.col("th_rows").desc_nulls_last())
)
