from pyspark.sql import functions as F

cleaned = (
    df_customers
    .filter((F.col("signup_date") > F.lit("2023-05-01").cast("date")) & (F.col("signup_date") < "2024-05-01"))
    .withColumn("name_prefix", F.substring("name", 1, 3))
    .withColumn("segment_prefix", F.substring("segment", 1, 2))
    .withColumn("segment_filled", F.coalesce("segment", F.lit("DIRECT")))
)

kept_keys = (
    cleaned
    .select("segment", "country")
    .intersectAll(
        df_customers
        .filter(F.col("is_active"))
        .select("segment", "country")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.segment") == F.col("k.segment")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

kept = (
    kept_rows
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.customer_id") == 15), "left_anti")
)

not_flagged = (
    kept
    .alias("s")
    .join(df_subscriptions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.customer_id") == 33), "left_anti")
)

valid = (
    not_flagged
    .filter(F.col("city").isNotNull())
    .select(
        "email",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "age",
        "name_prefix",
        "segment_prefix",
        F.initcap(F.trim("city")).alias("city_title"),
        F.initcap(F.trim("city")).alias("city_title_2"),
        F.coalesce("segment", F.lit("N/A")).alias("segment_filled_2"),
    )
)

filtered = (
    valid
    .filter(F.col("signup_date") > F.lit("2023-05-15").cast("date"))
    .select(
        "city",
        "segment",
        "signup_date",
        "age",
        "city_title_2",
        "segment_filled_2",
        F.initcap(F.trim("segment")).alias("segment_title"),
        F.when(F.col("city").isin(["Singapore", "Bangkok"]), "Priority").otherwise("Other").alias("city_group"),
        F.coalesce("city", F.lit("OTHER")).alias("resolved_city"),
    )
)

filtered_customers = (
    filtered
    .filter((F.col("signup_date") >= F.lit("2023-08-17").cast("date")) & F.col("city").isNotNull())
    .select("*", F.add_months("signup_date", 6).alias("signup_date_plus_months"))
)

result = (
    filtered_customers
    .groupBy(
        F.date_trunc("week", "signup_date").alias("week_start"),
        F.when(F.col("age") >= 73, "HIGH").otherwise("LOW").alias("age_tier"),
        "segment",
    )
    .agg(
        F.round(F.avg("age"), 2).alias("avg_age"),
        F.round(F.avg("age"), 2).alias("avg_age_2"),
        F.count(F.when(F.col("city") == "Bangkok", 1)).alias("bangkok_rows"),
        F.count(F.when(F.col("city") == "Berlin", 1)).alias("berlin_count"),
    )
    .filter(F.col("bangkok_rows") >= 1)
    .orderBy(F.col("bangkok_rows").desc())
)
