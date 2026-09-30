from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("subscription_plan").orderBy("started_at", "id", "subscription_id", "subscription_id_2")
w2 = Window.partitionBy("subscription_plan").orderBy("started_at", "id", "subscription_id", "subscription_id_2").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("subscription_plan").orderBy("subscription_monthly_fee", "id", "subscription_id", "subscription_id_2")
w4 = Window.partitionBy("browser").orderBy("subscription_monthly_fee", "id", "subscription_id", "subscription_id_2")
w5 = Window.partitionBy("country")
w6 = Window.partitionBy("subscription_plan")
w7 = Window.partitionBy(F.col("s.id"), F.col("s.subscription_id"), F.col("s.subscription_id_2")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

matched = (
    df_sessions
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & F.col("s.started_at").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

with_window = (
    matched
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & F.col("e.started_at").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id_2"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

gaps = (
    with_window
    .withColumn("is_new_burst", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "browser",
        "started_at",
        "country",
        "subscription_id",
        "subscription_id_2",
        "subscription_plan",
        "subscription_monthly_fee",
        F.sum("is_new_burst").over(w2).alias("island_id"),
    )
)

enriched = (
    islands
    .withColumn("subscription_monthly_fee_bucket", F.ntile(5).over(w3))
    .withColumn("event_seq", F.row_number().over(w4))
    .withColumn("country_stddev_samp_subscription_id", F.stddev_samp("subscription_id").over(w5))
    .withColumn("subscription_plan_min_subscription_monthly_fee", F.min("subscription_monthly_fee").over(w6))
)

latest_match = (
    enriched
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.browser",
        "s.started_at",
        "s.subscription_id",
        "s.subscription_id_2",
        "s.subscription_plan",
        "s.subscription_monthly_fee",
        "s.island_id",
        "s.subscription_monthly_fee_bucket",
        "s.event_seq",
        F.col("c.event_time").alias("web_event_event_time"),
        F.col("c.event_type").alias("web_event_event_type"),
    )
)

daily = (
    latest_match
    .groupBy(F.date_trunc("YEAR", "web_event_event_time").alias("year_start"))
    .agg(
        F.sum(F.when(F.col("browser") == "Edge", F.col("island_id"))).alias("edge_total_island_id"),
        F.count(F.when(F.col("subscription_plan") == "basic", 1)).alias("basic_count"),
        F.count_if(F.col("browser") == "Edge").alias("edge_events"),
    )
)

result = daily.select("year_start", "edge_total_island_id", "basic_count", "edge_events")
