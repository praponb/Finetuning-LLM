from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

with_latest = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_time"))
)

joined = (
    with_latest
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.name"), F.col("p.country").alias("customer_country"))
)

with_latest_sessions = (
    joined
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.duration_sec"), F.col("c.page").alias("web_event_page"))
)

with_last = (
    with_latest_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.duration_sec").alias("web_event_duration_sec"))
)

result = (
    with_last
    .groupBy("customer_id")
    .agg(
        F.min("started_at").alias("started_at"),
        F.percentile_approx("duration_sec", [0.25, 0.5, 0.75]).alias("duration_sec_quartiles"),
        F.round(F.avg("web_event_duration_sec"), 2).alias("avg_web_event_duration_sec"),
        F.count_if(F.col("device") == "tablet").alias("tablet_events"),
        F.round(F.avg("web_event_duration_sec"), 2).alias("avg_web_event_duration_sec_2"),
        F.max("web_event_duration_sec").alias("max_web_event_duration_sec"),
    )
    .filter(F.col("tablet_events") < 1000)
    .orderBy("avg_web_event_duration_sec_2")
)
