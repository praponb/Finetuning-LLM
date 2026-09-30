from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w2 = Window.orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_web_events
    .filter(F.col("duration_sec") <= 428)
    .select(
        "id",
        "session_id",
        "page",
        F.count("duration_sec").over(w).alias("duration_sec_count_7d"),
        F.sum("duration_sec").over(w2).alias("running_duration_sec"),
    )
)
