from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc(), "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id", "subscription_id")
w3 = Window.partitionBy("customer_id").orderBy("started_at", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

latest = (
    df_sessions
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

with_window = (
    latest
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.started_at") >= F.col("sub.start_date")) & (F.col("a.started_at") <= F.date_add(F.col("sub.end_date"), 3)), "inner")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

flagged = (
    with_window
    .withColumn("session_start_flag", F.when(F.col("started_at") - F.lag("started_at").over(w2) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessions_sessions = flagged.withColumn("session_no", F.sum("session_start_flag").over(w3))

eligible = (
    sessions_sessions
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") < F.col("s.started_at")), "left_semi")
)

result = (
    eligible
    .alias("src")
    .join(df_customers.alias("dim"), F.col("src.customer_id") == F.col("dim.id"), "inner")
    .cube(
        F.col("dim.country").alias("customer_country"),
        F.col("src.plan").alias("plan"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("src.session_start_flag")).alias("min_session_start_flag"),
    )
)
