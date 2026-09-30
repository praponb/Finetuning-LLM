from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time")

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "event_time",
        F.percent_rank().over(w).alias("event_time_pct_rank"),
    )
    .orderBy(F.col("id").desc())
)
