from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type", "session_id").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w2 = Window.partitionBy("session_id", "page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("event_type").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_web_events
    .select(
        "*",
        F.sum("duration_sec").over(w).alias("duration_sec_sum_90d"),
        F.sum("duration_sec").over(w2).alias("cumulative_duration_sec"),
        F.first("page", ignorenulls=True).over(w3).alias("first_known_page"),
    )
)
