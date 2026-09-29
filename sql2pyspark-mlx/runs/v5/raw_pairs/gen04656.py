from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("session_id").orderBy("duration_sec", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("session_id")

result = (
    df_web_events
    .filter(F.col("session_id").isin([9, 22, 23]))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        F.avg("duration_sec").over(w_part).alias("duration_sec_avg_2"),
        F.stddev_samp("duration_sec").over(w2).alias("session_id_stddev_samp_duration_sec"),
        F.avg("duration_sec").over(w_part).alias("duration_sec_avg_2_2"),
    )
)
