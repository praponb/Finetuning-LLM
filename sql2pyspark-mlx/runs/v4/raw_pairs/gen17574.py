from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id")
w2 = Window.partitionBy("session_id").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("session_id").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)

filtered = (
    df_web_events
    .filter(F.col("session_id").isin([14, 29, 37]))
    .select(
        "id",
        "session_id",
        "event_time",
        "duration_sec",
        F.datediff(F.current_date(), "event_time").alias("days_since_event_time"),
        F.coalesce("duration_sec", F.lit(0)).alias("duration_sec_filled"),
    )
)

result = (
    filtered
    .withColumn("session_id_min_duration_sec", F.min("duration_sec").over(w))
    .withColumn("cumulative_duration_sec", F.sum("duration_sec").over(w2))
    .withColumn("duration_sec_filled_avg_30d", F.avg("duration_sec_filled").over(w3))
)
