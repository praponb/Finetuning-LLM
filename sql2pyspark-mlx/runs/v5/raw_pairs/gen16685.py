from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc())
w2 = Window.partitionBy("session_id").orderBy("event_time")

result = (
    df_web_events
    .filter(F.hour("event_time").between(6, 10))
    .select(
        "id",
        "session_id",
        "duration_sec",
        "event_time",
        F.percent_rank().over(w).alias("duration_sec_pct_rank"),
        F.dense_rank().over(w2).alias("dense_rnk"),
    )
)
