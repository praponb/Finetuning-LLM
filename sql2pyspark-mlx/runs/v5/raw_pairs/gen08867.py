from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("device").orderBy("started_at", "id")
w3 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_sessions
    .filter((F.col("started_at") > F.to_timestamp(F.lit("2023-04-05 05:00:00"))) & F.col("country").isNotNull())
    .select("*", F.trim("country").alias("clean_country"))
)

with_last = (
    filtered
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.page").alias("web_event_page"))
)

kept = (
    with_last
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "left_anti")
)

flagged = (
    kept
    .withColumn("session_start_flag", F.when(F.col("started_at") - F.lag("started_at").over(w2) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        "web_event_page",
        F.sum("session_start_flag").over(w3).alias("burst_id"),
    )
)

attributed = (
    sessionized
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & (F.col("s.started_at") >= F.col("sub.start_date")) & (F.col("s.started_at") <= F.date_add(F.col("sub.end_date"), 7)), "left")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

result = (
    attributed
    .groupBy(
        F.when(F.col("monthly_fee") >= 249, "LARGE").otherwise("SMALL").alias("monthly_fee_tier"),
        F.date_trunc("WEEK", "started_at").alias("week_start"),
        "country",
    )
    .agg(
        F.sort_array(F.collect_set("device")).alias("device_list"),
        F.percentile_approx("burst_id", 0.95).alias("burst_id_p95"),
        F.count(F.when(F.col("web_event_page") == "/cart", 1)).alias("cart_count"),
    )
)
