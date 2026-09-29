from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type", "session_id").orderBy("event_time", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("session_id", "event_type")
w3 = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc_nulls_last())
w4 = Window.partitionBy("page").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w5 = Window.partitionBy("session_id", "page").orderBy("duration_sec", "id")

metrics = (
    df_web_events
    .select(
        "*",
        F.avg("duration_sec").over(w).alias("duration_sec_avg_6"),
        F.count("duration_sec").over(w2).alias("session_id_count_duration_sec"),
        F.percent_rank().over(w3).alias("duration_sec_percent_rank"),
        F.count("duration_sec").over(w4).alias("duration_sec_count_7d"),
        F.lead("duration_sec", 1).over(w5).alias("next_duration_sec"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("session_id_count_duration_sec") >= 17, "LARGE").otherwise("SMALL").alias("session_id_count_duration_sec_tier"),
    )
    .agg(
        F.avg("session_id_count_duration_sec").alias("avg_session_id_count_duration_sec"),
        F.sum(F.when(F.col("event_type") == "add_to_cart", F.col("session_id_count_duration_sec")).otherwise(0)).alias("add_to_cart_session_id_count_duration_sec"),
        F.countDistinct("session_id").alias("distinct_session_ids"),
    )
)
