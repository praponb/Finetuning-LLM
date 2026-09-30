from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())

with_latest = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_type"), F.col("c.page").alias("web_event_page"))
)

result = (
    with_latest
    .groupBy(F.date_trunc("WEEK", "started_at").alias("week_start"), "customer_id")
    .agg(
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_rows"),
        F.count("*").alias("cnt"),
        F.size(F.collect_set("device")).alias("num_devices"),
        F.count(F.when(F.col("browser") == "Safari", 1)).alias("safari_rows"),
    )
    .filter(F.col("safari_rows") > 1)
    .orderBy(F.col("customer_id").desc_nulls_last())
)
