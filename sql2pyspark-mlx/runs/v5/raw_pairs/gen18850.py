from pyspark.sql import functions as F

filtered = (
    df_customers
    .filter(F.col("signup_date") > F.lit("2023-03-18").cast("date"))
    .select(
        "id",
        "name",
        "email",
        "city",
        "segment",
        "is_active",
        "credit_limit",
        "age",
        F.round((F.col("credit_limit") / F.col("age")), 2).alias("credit_limit_per_age"),
        F.lower("city").alias("city_lower"),
    )
)

not_flagged = (
    filtered
    .alias("s")
    .join(df_subscriptions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.end_date") >= "2024-07-01"), "left_anti")
)

not_flagged_customers = (
    not_flagged
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") < "2024-07-08"), "left_semi")
)

remaining_keys = (
    not_flagged_customers
    .select("city", "id")
    .subtract(
        df_customers
        .filter(F.col("city").isin(["Berlin", "Tokyo"]))
        .select("city", "id")
    )
)

kept_rows = (
    not_flagged_customers
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

remaining_keys_2 = (
    kept_rows
    .select("city")
    .exceptAll(
        df_customers
        .filter(F.col("city").isin(["New York", "Bangkok", "Tokyo"]))
        .select("city")
    )
)

reconciled = (
    kept_rows
    .alias("s")
    .join(remaining_keys_2.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("credit_limit_per_age") >= 77, "PREMIUM").otherwise("STANDARD").alias("credit_limit_per_age_tier"),
    )
    .agg(
        F.size(F.collect_set("city")).alias("num_cities"),
        F.round(F.sum("credit_limit_per_age") / F.nullif(F.count(F.when(F.col("city") == "New York", 1)), F.lit(0)), 2).alias("credit_limit_per_age_per_new_york"),
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
        F.round(F.sum("age") / F.nullif(F.count(F.when(F.col("segment") == "SMB", 1)), F.lit(0)), 2).alias("age_per_smb"),
    )
    .orderBy("credit_limit_per_age_per_new_york")
)
