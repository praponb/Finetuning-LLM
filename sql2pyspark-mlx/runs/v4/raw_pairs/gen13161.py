from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("duration_sec", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("event_type", "session_id").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)

filtered = (
    df_web_events
    .filter(F.col("event_time") > F.lit("2023-12-11").cast("date"))
    .select("*", F.date_format("event_time", "yyyy-MM").alias("event_time_label"))
)

result = (
    filtered
    .select(
        "*",
        F.avg("duration_sec").over(w).alias("duration_sec_avg_2_prior"),
        F.count("duration_sec").over(w2).alias("duration_sec_count_7d"),
    )
)
