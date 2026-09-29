from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("page", "session_id").orderBy("event_time", "id")
w2 = Window.orderBy(F.col("duration_sec").desc(), "id")

result = (
    df_web_events
    .select(
        "*",
        F.lead("duration_sec").over(w_running).alias("next_duration_sec"),
        F.row_number().over(w_running).alias("row_seq"),
        F.row_number().over(w2).alias("seq_no"),
    )
)
