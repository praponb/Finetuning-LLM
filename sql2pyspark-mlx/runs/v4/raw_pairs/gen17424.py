from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())

with_latest = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.duration_sec").alias("web_event_duration_sec"))
)

result = (
    with_latest
    .groupBy("country")
    .agg(
        F.count(F.when(F.col("device") == "tablet", 1)).alias("tablet_count"),
        F.count(F.when(F.col("browser") == "Safari", 1)).alias("safari_count"),
    )
)
