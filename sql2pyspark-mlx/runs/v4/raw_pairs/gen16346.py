from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("started_at", "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("web_event_event_type").orderBy(F.col("row_seq").desc(), "id")
w6 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy(F.col("row_seq").desc(), "id")
w8 = Window.partitionBy("web_event_event_type").orderBy("started_at", "id").rowsBetween(-2, Window.currentRow)
w9 = Window.partitionBy("customer_id")
w10 = Window.partitionBy("browser").orderBy("started_at", "id")
w11 = Window.partitionBy("web_event_event_type").orderBy("started_at", "id")
w12 = Window.partitionBy("web_event_event_type").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w13 = Window.partitionBy("browser", "country").orderBy(F.col("seq_no").desc(), "id")

enriched = (
    df_sessions
    .select(
        "*",
        F.first("country", ignorenulls=True).over(w).alias("first_known_country"),
        F.last("browser", ignorenulls=True).over(w_grp).alias("last_known_browser"),
        F.last("browser", ignorenulls=True).over(w_grp).alias("last_known_browser_2"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

latest_match = (
    enriched
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_type").alias("web_event_event_type"))
)

with_windows = (
    latest_match
    .withColumn("seq_no", F.row_number().over(w5))
    .withColumn("cumulative_row_seq", F.sum("row_seq").over(w6))
    .withColumn("row_seq_bucket", F.ntile(3).over(w7))
    .withColumn("row_seq_avg_2", F.avg("row_seq").over(w8))
    .withColumn("customer_id_stddev_samp_row_seq", F.stddev_samp("row_seq").over(w9))
)

joined = (
    with_windows
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "inner")
    .select("e.*", F.col("c.credit_limit"))
)

gaps = (
    joined
    .withColumn("session_start_flag", F.when(F.col("started_at") - F.lag("started_at").over(w10) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessionized = gaps.withColumn("island_id", F.sum("session_start_flag").over(w6))

gaps_2 = (
    sessionized
    .withColumn("new_group_flag", F.when(F.col("started_at") - F.lag("started_at").over(w11) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_sessions = gaps_2.withColumn("session_id", F.sum("new_group_flag").over(w12))

deduped = (
    sessions_sessions
    .withColumn("dedup_rank", F.row_number().over(w13))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .groupBy("island_id")
    .agg(
        F.percentile_approx("island_id", [0.25, 0.5, 0.75]).alias("island_id_quartiles"),
        F.count("*").alias("cnt"),
        F.size(F.collect_set("device")).alias("num_devices"),
    )
    .filter(F.col("cnt") > 1)
    .orderBy(F.col("island_id").desc())
)
