from pyspark.sql import functions as F

kept_keys = (
    df_customers
    .select("city", "segment")
    .intersectAll(
        df_customers
        .filter(F.lower("name").like("%avi%"))
        .select("city", "segment")
    )
)

kept_rows = (
    df_customers
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.segment") == F.col("k.segment")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .withColumn("age_tier", F.when(F.col("age") >= 61, "HIGH").otherwise("LOW"))
    .withColumn("quarter_start", F.date_trunc("quarter", "signup_date"))
    .groupingSets(
        [["age_tier", "quarter_start"], ["age_tier"], []],
        "age_tier",
        "quarter_start",
    )
    .agg(
        F.percentile_approx("age", 0.95).alias("age_p95"),
        F.count_if(F.col("city") == "Chiang Mai").alias("chiang_mai_events"),
        F.corr("age", "credit_limit").alias("corr_age_credit_limit"),
        F.count("*").alias("event_count"),
        F.percentile_approx("age", [0.25, 0.5, 0.75]).alias("age_quartiles"),
        F.stddev_samp("age").alias("age_stddev"),
    )
    .orderBy(F.col("event_count").desc_nulls_last())
)
