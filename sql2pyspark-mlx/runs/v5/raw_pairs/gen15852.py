from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("quarter_start").orderBy(F.col("event_count").desc())

latest_match = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.customer_id", "s.device", "s.started_at", F.col("c.event_type"))
)

result = (
    latest_match
    .groupBy(
        F.date_trunc("QUARTER", "started_at").alias("quarter_start"),
        "customer_id",
    )
    .agg(
        F.count_if(F.col("event_type") == "click").alias("click_events"),
        F.count("*").alias("event_count"),
        F.size(F.collect_set("event_type")).alias("num_event_types"),
    )
    .withColumn("event_count_percent_rank", F.percent_rank().over(w2))
    .orderBy(F.col("event_count").desc())
)
