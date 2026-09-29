from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device", "customer_id").orderBy(F.col("started_at").desc(), "id")

with_windows = (
    df_sessions
    .withColumn("event_seq", F.row_number().over(w_running))
    .withColumn("seq_no", F.row_number().over(w2))
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("event_seq") >= 8, "HIGH").otherwise("LOW").alias("event_seq_tier"),
    )
    .agg(
        F.count_if(F.col("device") == "desktop").alias("desktop_events"),
        F.avg(F.when(F.col("browser") == "Firefox", F.col("event_seq"))).alias("firefox_avg_event_seq"),
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_count"),
        F.sum(F.when(F.col("device") == "desktop", F.col("event_seq")).otherwise(0)).alias("desktop_event_seq"),
        F.round(F.avg("event_seq"), 2).alias("avg_event_seq"),
    )
)
