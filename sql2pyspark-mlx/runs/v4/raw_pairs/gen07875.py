from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

latest_match = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_type").alias("web_event_event_type"))
)

result = (
    latest_match
    .withColumn("quarter_start", F.date_trunc("QUARTER", "started_at"))
    .groupingSets(
        [["quarter_start", "web_event_event_type"], ["quarter_start"]],
        "quarter_start",
        "web_event_event_type",
    )
    .agg(
        F.max("started_at").alias("ended_at"),
        F.max("started_at").alias("ended_at_2"),
        F.max("started_at").alias("ended_at_3"),
        F.min("started_at").alias("started_at"),
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") >= 1)
    .orderBy("row_count")
)
