from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id")
w5 = Window.partitionBy("customer_id").orderBy("session_no")
w6 = Window.partitionBy("device").orderBy("customer_id_min_session_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy(F.col("cumulative_session_no").asc_nulls_first())

cleaned = (
    df_sessions
    .filter(~F.col("country").eqNullSafe(F.lit("JP")))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        F.when(F.col("country") == "DE", 1).otherwise(0).alias("is_de"),
        F.coalesce("country", F.lit("OTHER")).alias("resolved_country"),
    )
)

joined = (
    cleaned
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id").eqNullSafe(F.col("d.id")), "left")
    .select(
        "s.id",
        "s.customer_id",
        "s.device",
        "s.started_at",
        "s.resolved_country",
        F.col("d.email"),
        F.col("d.city"),
        F.col("d.name"),
    )
)

flagged = (
    joined
    .withColumn("is_new_session", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "name",
        F.sum("is_new_session").over(w2).alias("session_no"),
    )
)

kept = (
    sessionized
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") >= F.col("s.started_at")), "left_anti")
)

with_windows = (
    kept
    .select(
        "id",
        "device",
        "name",
        F.sum("session_no").over(w_running).alias("cumulative_session_no"),
        F.min("session_no").over(w4).alias("customer_id_min_session_no"),
        F.percent_rank().over(w5).alias("session_no_percent_rank"),
    )
)

with_windows_sessions = (
    with_windows
    .withColumn("running_customer_id_min_session_no", F.sum("customer_id_min_session_no").over(w6))
    .withColumn("cumulative_session_no_percent_rank", F.percent_rank().over(w7))
)

stats = (
    with_windows_sessions
    .groupBy(
        F.when(F.col("cumulative_session_no_percent_rank") >= 0.76, "PREMIUM").otherwise("STANDARD").alias("cumulative_session_no_percent_rank_tier"),
        F.when(F.col("customer_id_min_session_no") >= 6, "PREMIUM").otherwise("STANDARD").alias("customer_id_min_session_no_tier"),
    )
    .agg(
        F.count(F.when(F.col("device") == "desktop", 1)).alias("desktop_rows"),
        F.countDistinct("device").alias("distinct_devices"),
        F.sort_array(F.collect_set("device")).alias("device_list"),
        F.percentile_approx("cumulative_session_no", 0.9).alias("cumulative_session_no_p90"),
        F.count("*").alias("num_rows"),
    )
)

result = (
    stats
    .select(
        "cumulative_session_no_percent_rank_tier",
        "customer_id_min_session_no_tier",
        "desktop_rows",
        "device_list",
    )
    .orderBy(F.col("desktop_rows").asc_nulls_last())
)
