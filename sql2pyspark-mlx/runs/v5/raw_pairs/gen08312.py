from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc_nulls_first())
w2 = Window.partitionBy("session_id").orderBy("duration_sec", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("session_id").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w5 = Window.orderBy(F.col("duration_sec").desc())

enriched = (
    df_web_events
    .select(
        "*",
        F.rank().over(w).alias("duration_sec_rank"),
        F.avg("duration_sec").over(w2).alias("duration_sec_avg_3"),
        F.avg("duration_sec").over(w3).alias("duration_sec_avg_6"),
        F.avg("duration_sec").over(w4).alias("duration_sec_avg_90d"),
        F.rank().over(w5).alias("duration_sec_rank_2"),
    )
)

result = (
    enriched
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "duration_sec",
        "duration_sec_avg_90d",
        "duration_sec_rank_2",
    )
    .orderBy("duration_sec_avg_90d")
)
