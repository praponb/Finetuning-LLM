from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("started_at_date").desc_nulls_first(), "id")
w2 = Window.partitionBy("plan").orderBy(F.col("rank_in_group").desc(), "id", "subscription_id")

base = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-05-16").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "browser",
        "country",
        F.trim("country").alias("clean_country"),
        F.to_date("started_at").alias("started_at_date"),
        F.coalesce("browser", F.lit("UNKNOWN")).alias("browser_clean"),
    )
)

latest = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

with_window = (
    latest
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.started_at_date") >= F.col("sub.start_date")) & (F.col("src.started_at_date") <= F.date_add(F.col("sub.end_date"), 7)), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

ranked = (
    with_window
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

valid = (
    ranked
    .filter(F.col("customer_id").isNotNull() & F.col("monthly_fee").between(100, 200))
    .withColumn("country_lower", F.lower("country"))
    .withColumn("resolved_country", F.coalesce("country", F.lit("NONE")))
)

remaining_keys = (
    valid
    .select("browser", "id")
    .exceptAll(
        df_sessions
        .filter(F.col("started_at") < "2025-06-16")
        .select("browser", "id")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.browser") == F.col("k.browser")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    kept_rows
    .filter(F.col("monthly_fee") <= 60)
    .select(
        "id",
        "customer_id",
        "country",
        "clean_country",
        "started_at_date",
        "rank_in_group",
        "plan",
        "monthly_fee",
        "country_lower",
        "resolved_country",
        F.when(F.col("plan") == "enterprise", 1).otherwise(0).alias("is_enterprise"),
        F.date_format("started_at_date", "yyyy-MM-dd").alias("started_at_date_label"),
    )
)

totals = (
    cleaned
    .groupBy(
        F.date_trunc("WEEK", "started_at_date").alias("week_start"),
        F.when(F.col("is_enterprise") >= 86, "HIGH").otherwise("LOW").alias("is_enterprise_tier"),
    )
    .agg(
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_rows"),
        F.percentile_approx("is_enterprise", 0.5).alias("is_enterprise_p50"),
        F.count_if(F.col("country") == "DE").alias("de_events"),
        F.percentile_approx("is_enterprise", 0.5).alias("is_enterprise_p50_2"),
        F.count_if(F.col("country") == "DE").alias("de_events_2"),
    )
    .filter(F.col("enterprise_rows") > 1)
)

result = totals.select("week_start", "is_enterprise_tier", "enterprise_rows", "de_events")
