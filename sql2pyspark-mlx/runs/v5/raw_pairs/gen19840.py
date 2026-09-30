from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy(F.col("is_singapore").desc_nulls_first(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.end_date").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("country").orderBy(F.col("subscription_start_date").desc_nulls_first(), "id")

cleaned = (
    df_customers
    .filter(F.col("signup_date") >= F.lit("2023-07-25").cast("date"))
    .select(
        "id",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.when(F.col("city") == "Singapore", 1).otherwise(0).alias("is_singapore"),
    )
)

ranked = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

latest_match = (
    ranked
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.email",
        "s.country",
        "s.credit_limit",
        "s.age",
        "s.dedup_rank",
        F.col("c.start_date").alias("subscription_start_date"),
        F.col("c.monthly_fee").alias("subscription_monthly_fee"),
    )
)

kept = (
    latest_match
    .alias("s")
    .join(df_sessions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.started_at") >= F.col("s.subscription_start_date")), "left_semi")
)

not_flagged = (
    kept
    .alias("s")
    .join(df_accounts.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.opened_date") <= F.col("s.subscription_start_date")), "left_anti")
)

base = (
    not_flagged
    .filter(F.col("subscription_start_date").between("2023-07-01", "2024-12-01"))
    .select(
        "id",
        "email",
        "country",
        "age",
        "dedup_rank",
        "subscription_start_date",
        F.upper("country").alias("country_upper"),
    )
)

latest = (
    base
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

stats = (
    latest
    .groupBy(
        F.date_trunc("year", "subscription_start_date").alias("year_start"),
        "country",
        F.when(F.col("age") >= 59, "LARGE").otherwise("SMALL").alias("age_tier"),
    )
    .agg(
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_count"),
        F.percentile_approx("age", 0.5).alias("age_p50"),
    )
)

result = (
    stats
    .select("year_start", "country", "age_tier", "jp_count", "age_p50")
    .orderBy(F.col("country").desc_nulls_last())
)
