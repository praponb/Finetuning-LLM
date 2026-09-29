from pyspark.sql import functions as F

base = (
    df_sessions
    .filter((F.col("started_at") > F.to_date(F.lit("2023-03-19"))) & (F.col("country") == "DE"))
    .withColumn("started_at_hour", F.hour("started_at"))
    .withColumn("country_length", F.length("country"))
)

matched = (
    base
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & (F.col("s.started_at") >= F.col("sub.start_date")) & (F.col("s.started_at") <= F.date_add(F.col("sub.end_date"), 10)), "left")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

matched_keys = (
    matched
    .select("device", "customer_id")
    .exceptAll(
        df_sessions
        .filter(~F.col("device").isin(["mobile", "tablet"]))
        .select("device", "customer_id")
    )
)

verified = (
    matched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.device") == F.col("k.device")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

kept_keys = (
    verified
    .select("browser", "customer_id")
    .subtract(
        df_sessions
        .filter(F.col("customer_id") == 10)
        .select("browser", "customer_id")
    )
)

kept_rows = (
    verified
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.browser") == F.col("k.browser")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.name"), F.col("p.age"), F.col("p.credit_limit"))
)

enriched_sessions = (
    enriched
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.country").alias("customer_country"))
)

result = (
    enriched_sessions
    .groupBy(
        F.when(F.col("credit_limit") >= 13100, "LARGE").otherwise("SMALL").alias("credit_limit_tier"),
        F.date_trunc("QUARTER", "started_at").alias("quarter_start"),
        "country",
    )
    .agg(
        F.countDistinct("country").alias("distinct_countries"),
        F.round(F.sum("monthly_fee") / F.nullif(F.count(F.when(F.col("country") == "SG", 1)), F.lit(0)), 2).alias("monthly_fee_per_sg"),
        F.min("subscription_id").alias("min_subscription_id"),
    )
    .orderBy(F.col("min_subscription_id").desc())
)
