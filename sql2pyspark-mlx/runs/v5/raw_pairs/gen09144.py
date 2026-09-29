from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())

latest_match = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.event_type").alias("web_event_event_type"),
        F.col("c.duration_sec"),
    )
)

result = (
    latest_match
    .groupBy(F.date_trunc("quarter", "started_at").alias("quarter_start"))
    .agg(
        F.min("started_at").alias("first_started_at"),
        F.sort_array(F.collect_set("device")).alias("device_list"),
        F.max("started_at").alias("last_seen"),
        F.sum(F.when(F.col("country") == "SG", F.col("duration_sec"))).alias("sg_total_duration_sec"),
    )
    .orderBy(F.col("quarter_start").desc_nulls_last())
)
