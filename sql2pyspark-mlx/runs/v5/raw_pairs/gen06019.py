from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("log_ts", "id", "k")
w2 = Window.partitionBy("service").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("k", "service").orderBy("log_ts", "id", "k")
w4 = Window.partitionBy("k").orderBy("is_new_session", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("k").orderBy(F.col("log_ts").desc(), "id", "k")
w_hist = Window.partitionBy("service").orderBy("session_no", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("host", "k").orderBy("session_no", "id", "k").rowsBetween(-3, Window.currentRow)
w8 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)

flattened = (
    df_app_logs
    .select("id", "service", "host", "log_ts", F.explode("context").alias("k", "v"))
)

with_gaps = (
    flattened
    .withColumn("is_new_session", F.when(F.col("log_ts") - F.lag("log_ts").over(w) > F.expr("INTERVAL 15 MINUTES"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

scored = (
    sessionized
    .withColumn("next_session_no", F.lead("session_no", 1).over(w3))
    .withColumn("last_known_is_new_session", F.last("is_new_session", ignorenulls=True).over(w4))
    .withColumn("event_seq", F.row_number().over(w5))
)

enriched = (
    scored
    .filter(F.col("session_no") > 7)
    .select(
        "*",
        F.sum("last_known_is_new_session").over(w_hist).alias("running_last_known_is_new_session"),
        F.avg("session_no").over(w7).alias("session_no_avg_3"),
        F.avg("event_seq").over(w8).alias("event_seq_avg_15m"),
    )
)

grouped = (
    enriched
    .withColumn("event_seq_tier", F.when(F.col("event_seq") >= 8, "PREMIUM").otherwise("STANDARD"))
    .withColumn("session_no_avg_3_tier", F.when(F.col("session_no_avg_3") >= 7, "PREMIUM").otherwise("STANDARD"))
    .cube("event_seq_tier", "session_no_avg_3_tier")
    .agg(
        F.max("log_ts").alias("last_seen"),
        F.percentile_approx("is_new_session", [0.25, 0.5, 0.75]).alias("is_new_session_quartiles"),
        F.max("log_ts").alias("last_seen_2"),
        F.max("log_ts").alias("ended_at"),
        F.grouping_id().alias("grouping_level"),
    )
)

result = (
    grouped
    .select(
        "session_no_avg_3_tier",
        "last_seen",
        "is_new_session_quartiles",
        "grouping_level",
    )
    .orderBy(F.col("grouping_level").desc(), "session_no_avg_3_tier")
)
