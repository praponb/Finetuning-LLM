from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc(), "id")
w3 = Window.partitionBy("page").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("session_id").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w5 = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc_nulls_first())
w6 = Window.orderBy("duration_sec_dense_rank", "id").rowsBetween(-6, Window.currentRow)
w7 = Window.partitionBy("event_type").orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("event_type").orderBy("cumulative_duration_sec", "id")
w9 = Window.partitionBy("page").orderBy("duration_sec_avg_7d", "id").rowsBetween(-5, Window.currentRow)
w10 = Window.partitionBy("session_id")
w11 = Window.partitionBy("session_id").orderBy("event_time", "id")
w12 = Window.orderBy(F.col("duration_sec_avg_7d").desc())
w13 = Window.partitionBy("session_id").orderBy(F.col("last_known_duration_sec_avg_7d").desc())
w14 = Window.orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_web_events
    .select(
        "*",
        F.sum("duration_sec").over(w).alias("cumulative_duration_sec"),
        F.row_number().over(w2).alias("row_seq"),
        F.first("duration_sec", ignorenulls=True).over(w3).alias("first_known_duration_sec"),
        F.avg("duration_sec").over(w4).alias("duration_sec_avg_7d"),
        F.dense_rank().over(w5).alias("duration_sec_dense_rank"),
    )
)

enriched = (
    metrics
    .withColumn("cumulative_duration_sec_avg_6", F.avg("cumulative_duration_sec").over(w6))
    .withColumn("last_known_duration_sec_avg_7d", F.last("duration_sec_avg_7d", ignorenulls=True).over(w7))
    .withColumn("seq_no", F.row_number().over(w8))
    .withColumn("duration_sec_dense_rank_avg_5", F.avg("duration_sec_dense_rank").over(w9))
)

enriched_web_events = (
    enriched
    .withColumn("session_id_max_duration_sec", F.max("duration_sec").over(w10))
    .withColumn("next_cumulative_duration_sec", F.lead("cumulative_duration_sec", 1).over(w11))
    .withColumn("duration_sec_avg_7d_rank", F.rank().over(w12))
    .withColumn("last_known_duration_sec_avg_7d_percent_rank", F.percent_rank().over(w13))
    .withColumn("cumulative_duration_sec_dense_rank_avg_5", F.sum("duration_sec_dense_rank_avg_5").over(w14))
)

joined = (
    enriched_web_events
    .alias("e")
    .join(df_sessions.alias("p"), F.col("e.session_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.device"), F.col("p.country"))
)

result = (
    joined
    .select(
        "id",
        "page",
        "event_time",
        "first_known_duration_sec",
        "duration_sec_dense_rank",
        "cumulative_duration_sec_avg_6",
        "seq_no",
        "duration_sec_dense_rank_avg_5",
        "session_id_max_duration_sec",
    )
    .orderBy(
        F.col("page").asc_nulls_first(),
        F.col("duration_sec_dense_rank_avg_5").desc(),
    )
)
