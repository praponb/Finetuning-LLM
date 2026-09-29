from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("duration_sec", "id")
w2 = Window.partitionBy("session_id").orderBy("next_duration_sec_times_prev_duration_sec", "id")
w3 = Window.partitionBy("session_id").orderBy(F.col("rank_in_group").desc(), "id")

enriched = (
    df_web_events
    .filter(F.col("event_time") < "2023-03-01")
    .select(
        "id",
        "session_id",
        "event_type",
        F.lead("duration_sec").over(w).alias("next_duration_sec"),
        F.lag("duration_sec").over(w).alias("prev_duration_sec"),
        F.row_number().over(w).alias("seq_no"),
    )
)

valid = (
    enriched
    .filter(F.col("session_id").isin([4, 13]))
    .select(
        "id",
        "session_id",
        "prev_duration_sec",
        "seq_no",
        (F.col("next_duration_sec") * F.col("prev_duration_sec")).alias("next_duration_sec_times_prev_duration_sec"),
    )
)

ranked = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

ranked_web_events = (
    ranked
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked_web_events
    .rollup("seq_no")
    .agg(
        F.min("seq_no").alias("min_seq_no"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("prev_duration_sec", 0.95).alias("prev_duration_sec_p95"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.avg("seq_no").alias("avg_seq_no"),
        F.countDistinct("session_id").alias("distinct_session_ids"),
    )
    .filter(F.col("distinct_ids_2") > 1)
)
