from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("started_at", "id", "subscription_id")
w2 = Window.partitionBy("plan").orderBy("started_at", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device").orderBy("started_at", "id", "subscription_id")
w4 = Window.partitionBy("device").orderBy("started_at", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.col("s.id"), F.col("s.subscription_id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w6 = Window.partitionBy("page").orderBy("started_at", "id", "subscription_id").rowsBetween(-2, Window.currentRow)
w7 = Window.orderBy(F.col("session_no").desc(), "id", "subscription_id")

cleaned = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-03-08 07:00:00").cast("timestamp")) & (F.to_date("started_at") >= "2025-04-27"))
    .withColumn("country_group", F.when(F.col("country").isin(["US", "SG"]), "Priority").otherwise("Other"))
)

with_window = (
    cleaned
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.started_at") >= F.col("sub.start_date")) & (F.col("src.started_at") < F.col("sub.end_date") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

with_prev = with_window.withColumn("prev_started_at", F.lag("started_at").over(w))

flagged = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("session_start_flag").over(w2))

with_gaps = (
    sessionized
    .withColumn("is_new_session", F.when(F.col("started_at") - F.lag("started_at").over(w3) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "subscription_id",
        "prev_started_at",
        "session_no",
        F.sum("is_new_session").over(w4).alias("session_id"),
    )
)

eligible = (
    islands
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") <= F.col("s.prev_started_at")), "left_anti")
)

latest_match = (
    eligible
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.started_at",
        "s.subscription_id",
        "s.session_no",
        F.col("c.event_time"),
        F.col("c.page"),
    )
)

with_windows = (
    latest_match
    .withColumn("last3_avg_session_no", F.avg("session_no").over(w6))
    .withColumn("last3_min_session_no", F.min("session_no").over(w6))
    .withColumn("last3_count_session_no", F.count("session_no").over(w6))
    .withColumn("session_no_bucket", F.ntile(4).over(w7))
)

result = (
    with_windows
    .select(
        "id",
        "started_at",
        "subscription_id",
        "session_no",
        "page",
        "last3_min_session_no",
        "last3_count_session_no",
        "session_no_bucket",
    )
    .orderBy(F.col("started_at").desc(), "page")
)
