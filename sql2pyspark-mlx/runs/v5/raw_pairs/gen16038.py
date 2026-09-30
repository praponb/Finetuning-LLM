from pyspark.sql import functions as F

remaining_keys = (
    df_sessions
    .select("device", "country")
    .intersect(
        df_sessions
        .filter(F.col("customer_id").isin([20, 38]))
        .select("device", "country")
    )
)

verified = (
    df_sessions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.device") == F.col("k.device")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

joined = (
    verified
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "inner")
    .filter(F.col("d.age") != 53)
    .select(
        "src.id",
        "src.customer_id",
        "src.device",
        "src.browser",
        "src.started_at",
        "src.country",
        F.col("d.age"),
    )
)

enriched = (
    joined
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .select(
        "s.customer_id",
        "s.device",
        "s.browser",
        "s.country",
        "s.age",
        F.col("c.signup_date"),
        F.col("c.city"),
    )
)

joined_sessions = (
    enriched
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .select(
        "e.customer_id",
        "e.device",
        "e.browser",
        "e.country",
        "e.age",
        "e.signup_date",
        "e.city",
        F.col("c.signup_date").alias("customer_signup_date"),
        F.col("c.segment"),
        F.col("c.name"),
    )
)

result = (
    joined_sessions
    .groupBy(F.date_trunc("MONTH", "customer_signup_date").alias("activity_month"))
    .agg(
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
        F.percentile_approx("age", 0.5).alias("age_p50"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") < 1000)
    .orderBy(F.col("row_count").desc())
)
