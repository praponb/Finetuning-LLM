from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc())
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "duration_sec",
        "event_time",
        F.percent_rank().over(w).alias("duration_sec_pct_rank"),
        F.round(F.avg("duration_sec").over(w2), 2).alias("duration_sec_moving_avg_7"),
    )
    .orderBy(F.col("duration_sec_moving_avg_7").desc(), "duration_sec_pct_rank")
)
