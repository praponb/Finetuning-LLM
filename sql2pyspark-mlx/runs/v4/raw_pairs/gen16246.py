from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("duration_sec", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.orderBy("is_new_session", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("event_type", "session_id")
w6 = Window.partitionBy("session_id", "event_type").orderBy("is_new_session", "id")
w_ordered = Window.orderBy(F.col("is_new_session_avg_2").desc(), "id")
w8 = Window.partitionBy("session_id", "event_type").orderBy("running_duration_sec", "id").rowsBetween(-5, Window.currentRow)
w9 = Window.partitionBy("page").orderBy("is_new_session_bucket", "id").rowsBetween(-5, Window.currentRow)
w10 = Window.partitionBy("device").orderBy("duration_sec", "id")
w_recent = Window.orderBy("session_no", "id").rowsBetween(-2, -1)

flagged = (
    df_web_events
    .withColumn("is_new_session", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessions_web_events = flagged.withColumn("session_no", F.sum("is_new_session").over(w2))

joined = (
    sessions_web_events
    .alias("a")
    .join(df_sessions.alias("s"), F.col("a.session_id") == F.col("s.id"), "left")
    .select("a.*", F.col("s.device"))
)

with_windows = (
    joined
    .select(
        "*",
        F.avg("is_new_session").over(w3).alias("is_new_session_avg_2"),
        F.sum("duration_sec").over(w4).alias("running_duration_sec"),
        F.min("session_no").over(w5).alias("event_type_min_session_no"),
        F.ntile(4).over(w6).alias("is_new_session_bucket"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.row_number().over(w_ordered).alias("row_seq"),
        F.avg("duration_sec").over(w8).alias("duration_sec_avg_5"),
        F.avg("event_type_min_session_no").over(w9).alias("event_type_min_session_no_avg_5"),
        F.row_number().over(w10).alias("seq_no"),
        F.avg("event_type_min_session_no").over(w_recent).alias("event_type_min_session_no_avg_2_prior"),
    )
)

stats = (
    metrics
    .groupBy(
        "seq_no",
        F.when(F.col("duration_sec") >= 380, "HIGH").otherwise("LOW").alias("duration_sec_tier"),
    )
    .agg(
        F.count(F.when(F.col("device") == "mobile", 1)).alias("mobile_rows"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("span_minutes"),
        F.count_if(F.col("device") == "tablet").alias("tablet_events"),
        F.count(F.when(F.col("page") == "/checkout", 1)).alias("checkout_rows"),
        F.round(F.avg("seq_no"), 2).alias("avg_seq_no"),
        F.count("*").alias("row_count"),
    )
)

result = (
    stats
    .select(
        "seq_no",
        "duration_sec_tier",
        "mobile_rows",
        "span_minutes",
        "checkout_rows",
        "avg_seq_no",
        "row_count",
    )
)
