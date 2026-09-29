from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city")
w2 = Window.partitionBy("segment").orderBy(F.col("age").desc(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())

scored = (
    df_customers
    .select(
        "*",
        F.corr("credit_limit", "age").over(w).alias("corr_credit_limit_age"),
        F.ntile(10).over(w2).alias("age_bucket"),
    )
)

not_flagged = (
    scored
    .alias("s")
    .join(df_subscriptions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.status").isin(["ACTIVE", "EXPIRED"])), "left_anti")
)

cleaned = (
    not_flagged
    .filter(F.col("city").isNotNull() & (F.col("credit_limit") > 10900))
    .select(
        "id",
        "name",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "age",
        "age_bucket",
        F.datediff(F.current_date(), "signup_date").alias("days_since_signup_date"),
        F.concat_ws("-", "segment", "email").alias("segment_email_key"),
        F.coalesce("segment", F.lit("OTHER")).alias("resolved_segment"),
    )
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.country").alias("session_country"),
        F.col("c.browser").alias("session_browser"),
    )
)

matched_keys = (
    with_latest
    .select("country", "segment")
    .intersectAll(
        df_customers
        .filter(F.col("email").isNotNull())
        .select("country", "segment")
    )
)

reconciled = (
    with_latest
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.segment") == F.col("k.segment")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(F.date_trunc("quarter", "signup_date").alias("quarter_start"))
    .agg(
        F.sum(F.when(F.col("segment") == "Corporate", F.col("days_since_signup_date"))).alias("corporate_total_days_since_signup_date"),
        F.count("*").alias("event_count"),
        F.sum(F.when(F.col("session_browser") == "Edge", F.col("age"))).alias("edge_total_age"),
        F.round(F.avg("age_bucket"), 2).alias("avg_age_bucket"),
        F.percentile_approx("age_bucket", 0.95).alias("age_bucket_p95"),
        F.sort_array(F.collect_set("session_browser")).alias("session_browser_list"),
    )
    .orderBy(F.col("edge_total_age").desc_nulls_last())
)
