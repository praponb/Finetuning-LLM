from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("browser").orderBy("credit_limit", "id")
w4 = Window.partitionBy("browser").orderBy("island_id", "id")
w_running_2 = Window.partitionBy("device").orderBy("days_since_started_at", "id").rowsBetween(-5, -1)
w6 = Window.orderBy(F.col("is_new_session").desc(), "id")
w7 = Window.partitionBy("device").orderBy("days_since_started_at_avg_5_prior", "id")

valid = (
    df_sessions
    .filter(F.col("browser").isNotNull() & (F.col("started_at") >= F.lit("2023-07-01 18:00:00").cast("timestamp")))
    .select(
        "*",
        F.datediff(F.current_date(), "started_at").alias("days_since_started_at"),
        F.coalesce("country", F.lit("UNKNOWN")).alias("country_filled"),
    )
)

with_gaps = (
    valid
    .withColumn("is_new_session", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_sessions = with_gaps.withColumn("island_id", F.sum("is_new_session").over(w2))

joined = (
    sessions_sessions
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .select("a.*", F.col("c.signup_date"), F.col("c.credit_limit"), F.col("c.name"))
)

eligible = (
    joined
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") >= F.col("s.started_at")), "left_semi")
)

with_windows = (
    eligible
    .select(
        "id",
        "customer_id",
        "device",
        "island_id",
        "name",
        F.lead("island_id", 1).over(w).alias("next_island_id"),
        F.lead("island_id", 1).over(w_running).alias("next_island_id_2"),
        F.row_number().over(w4).alias("seq_no"),
        F.avg("days_since_started_at").over(w_running_2).alias("days_since_started_at_avg_5_prior"),
        F.ntile(4).over(w6).alias("is_new_session_bucket"),
    )
)

deduped = (
    with_windows
    .withColumn("latest_rank", F.row_number().over(w7))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .rollup(
        F.when(F.col("next_island_id_2") >= 8, "PREMIUM").otherwise("STANDARD").alias("next_island_id_2_tier"),
        "customer_id",
    )
    .agg(
        F.avg(F.when(F.col("device") == "tablet", F.col("is_new_session_bucket"))).alias("tablet_avg_is_new_session_bucket"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("island_id").alias("total_island_id"),
        F.count("*").alias("cnt"),
        F.sum("seq_no").alias("total_seq_no"),
        F.sum(F.when(F.col("device") == "desktop", F.col("island_id")).otherwise(0)).alias("desktop_island_id"),
    )
    .filter(F.col("tablet_avg_is_new_session_bucket") > 0)
)
