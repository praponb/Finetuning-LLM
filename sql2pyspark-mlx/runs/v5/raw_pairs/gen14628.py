from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("device", "browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device").orderBy("monthly_fee", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("quarter_start").orderBy(F.col("th_rows").desc())

enriched = (
    df_sessions
    .withColumn("last_known_browser", F.last("browser", ignorenulls=True).over(w_recent))
)

with_window = (
    enriched
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & F.col("src.started_at").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

not_flagged = (
    with_window
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.duration_sec") != 293), "left_anti")
)

not_flagged_sessions = (
    not_flagged
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "left_anti")
)

eligible = (
    not_flagged_sessions
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.page") == "/checkout"), "left_anti")
)

enriched_sessions = (
    eligible
    .filter(~F.col("device").isin(["tablet", "desktop"]))
    .select(
        "*",
        F.count("monthly_fee").over(w2).alias("running_count_monthly_fee"),
        F.sum("monthly_fee").over(w2).alias("running_sum_monthly_fee"),
        F.max("monthly_fee").over(w2).alias("running_max_monthly_fee"),
    )
)

result = (
    enriched_sessions
    .groupBy(
        F.date_trunc("quarter", "started_at").alias("quarter_start"),
        F.date_trunc("week", "started_at").alias("week_start"),
    )
    .agg(
        F.min("started_at").alias("started_at"),
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_rows"),
    )
    .withColumn("th_rows_dense_rank", F.dense_rank().over(w3))
    .orderBy("th_rows")
)
