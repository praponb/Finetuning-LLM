from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("page")

result = (
    df_web_events
    .select(
        "*",
        F.sum("duration_sec").over(w).alias("cumulative_duration_sec"),
        F.sum("duration_sec").over(w_running).alias("page_sum_duration_sec"),
    )
)
