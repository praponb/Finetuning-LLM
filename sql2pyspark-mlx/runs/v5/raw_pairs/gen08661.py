from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "segment")
w2 = Window.orderBy(F.col("age").asc_nulls_last())
w3 = Window.partitionBy("segment").orderBy("corr_credit_limit_age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("segment").orderBy(F.col("credit_limit").desc(), "id")
w5 = Window.partitionBy("segment").orderBy(F.col("corr_credit_limit_age").asc_nulls_first())
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())

kept = (
    df_customers
    .alias("s")
    .join(df_subscriptions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.status") != "ACTIVE"), "left_anti")
)

metrics = (
    kept
    .select(
        "id",
        "name",
        "email",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        F.corr("credit_limit", "age").over(w).alias("corr_credit_limit_age"),
        F.cume_dist().over(w2).alias("age_cume_dist"),
    )
)

not_flagged = (
    metrics
    .alias("s")
    .join(df_sessions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.started_at") >= F.col("s.signup_date")), "left_semi")
)

eligible = (
    not_flagged
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.order_date") < F.col("s.signup_date")), "left_semi")
)

metrics_customers = (
    eligible
    .select(
        "id",
        "name",
        "email",
        "signup_date",
        "corr_credit_limit_age",
        "age_cume_dist",
        F.first("city", ignorenulls=True).over(w3).alias("first_known_city"),
        F.ntile(5).over(w4).alias("credit_limit_bucket"),
        F.cume_dist().over(w5).alias("corr_credit_limit_age_cume_dist"),
    )
)

latest_match = (
    metrics_customers
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.country").alias("session_country"))
)

stats = (
    latest_match
    .groupBy(
        F.date_trunc("YEAR", "signup_date").alias("year_start"),
        "session_country",
        F.when(F.col("credit_limit_bucket") >= 7, "HIGH").otherwise("LOW").alias("credit_limit_bucket_tier"),
    )
    .agg(
        F.countDistinct("session_country").alias("distinct_session_countries"),
        F.count(F.when(F.col("session_country") == "JP", 1)).alias("jp_count"),
        F.round(F.sum("credit_limit_bucket") / F.nullif(F.count(F.when(F.col("session_country") == "US", 1)), F.lit(0)), 2).alias("credit_limit_bucket_per_us"),
        F.percentile_approx("corr_credit_limit_age", 0.9).alias("corr_credit_limit_age_p90"),
        F.count_if(F.col("session_country") == "UK").alias("uk_events"),
    )
)

result = (
    stats
    .select(
        "year_start",
        "session_country",
        "credit_limit_bucket_tier",
        "credit_limit_bucket_per_us",
        "uk_events",
        F.when(F.col("session_country").rlike("^(JP|SG)$"), "GROUP_B").when(F.col("session_country").rlike("^(TH|US)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("session_country_zone"),
    )
)
