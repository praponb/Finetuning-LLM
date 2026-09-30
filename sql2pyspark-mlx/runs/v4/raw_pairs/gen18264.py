from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.min("monthly_fee").alias("min_subscription_monthly_fee"))

filtered = (
    df_sessions
    .filter(F.col("started_at") > F.lit("2023-09-13 18:00:00").cast("timestamp"))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "country",
        F.upper("country").alias("country_upper"),
    )
)

filtered_sessions = (
    filtered
    .filter(F.col("country").isNotNull() & (F.col("customer_id") == 19))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "country",
        "country_upper",
        F.concat_ws("-", "device", "browser").alias("device_browser_key"),
        F.coalesce("country", F.lit("N/A")).alias("country_filled"),
    )
)

with_latest = (
    filtered_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_type"))
)

with_last = (
    with_latest
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.page"))
)

with_last_sessions = (
    with_last
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.duration_sec"), F.col("c.page").alias("web_event_page"))
)

result = (
    with_last_sessions
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "device",
        "device_browser_key",
        "country_filled",
        "event_type",
        "duration_sec",
        "web_event_page",
        F.col("min_subscription_monthly_fee"),
    )
    .orderBy(F.col("id").asc_nulls_last())
    .limit(20)
)
