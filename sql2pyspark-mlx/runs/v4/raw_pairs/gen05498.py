from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_web_events
    .filter((F.col("event_time") >= F.to_timestamp(F.lit("2023-08-17 22:00:00"))) & (F.col("event_time") >= "2023-04-01"))
    .withColumn("duration_sec_adjusted", F.round((F.col("duration_sec") * 1.1), 2))
    .withColumn("duration_sec_tier", F.when(F.col("duration_sec") >= 513, "High").when(F.col("duration_sec") >= 132, "Medium").otherwise("Low"))
)

flagged = (
    cleaned
    .withColumn("is_new_session", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

islands = flagged.withColumn("burst_id", F.sum("is_new_session").over(w2))

result = (
    islands
    .groupBy("event_type")
    .agg(
        F.countDistinct("page").alias("distinct_pages"),
        F.countDistinct("page").alias("distinct_pages_2"),
        F.sum(F.when(F.col("page") == "/home", F.col("burst_id")).otherwise(0)).alias("home_burst_id"),
        F.min("event_time").alias("started_at"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("duration_min"),
    )
    .filter(F.col("duration_min") > 0)
)
