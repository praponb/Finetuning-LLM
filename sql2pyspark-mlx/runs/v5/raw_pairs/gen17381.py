from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id"), F.col("s.subscription_id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("event_type").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.partitionBy("event_type")
w4 = Window.partitionBy("event_type", "plan").orderBy("monthly_fee", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

kept = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.duration_sec").isNotNull()), "left_anti")
)

attributed = (
    kept
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.started_at") >= F.col("sub.start_date")) & (F.col("src.started_at") <= F.date_add(F.col("sub.end_date"), 3)), "inner")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

with_latest = (
    attributed
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.event_type"),
        F.col("c.duration_sec").alias("web_event_duration_sec"),
    )
)

scored = (
    with_latest
    .filter(F.col("device").isin(["tablet", "mobile", "desktop"]))
    .select(
        "*",
        F.sum("web_event_duration_sec").over(w2).alias("web_event_duration_sec_sum_30d"),
        F.sum("monthly_fee").over(w3).alias("event_type_sum_monthly_fee"),
        F.sum("web_event_duration_sec").over(w4).alias("cumulative_web_event_duration_sec"),
    )
)

result = (
    scored
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        "subscription_id",
        "plan",
        "monthly_fee",
    )
    .orderBy(F.col("id").asc_nulls_last())
)
