from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("event_time").asc_nulls_first(), "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id")
w3 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("page").orderBy(F.col("latest_rank_times_duration_sec").desc(), "id")

ranked = (
    df_web_events
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

cleaned = (
    ranked
    .filter((F.col("event_time") > F.to_date(F.lit("2024-03-01"))) & F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        "latest_rank",
        (F.col("latest_rank") * F.col("duration_sec")).alias("latest_rank_times_duration_sec"),
    )
)

flagged = (
    cleaned
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w2).isNull() | (F.col("event_time") > F.lag("event_time").over(w2) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_web_events = flagged.withColumn("island_id", F.sum("session_start_flag").over(w3))

ranked_web_events = (
    sessions_web_events
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
)

result = (
    ranked_web_events
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "latest_rank",
        "session_start_flag",
        "island_id",
        "rn",
    )
    .orderBy(F.col("event_time").desc_nulls_last())
)
