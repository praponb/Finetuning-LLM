from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("event_type").orderBy("is_new_session", "id")
w4 = Window.partitionBy("event_type").orderBy("rank_in_group_whole", "id").rowsBetween(-3, -1)
w_running = Window.partitionBy("session_id").orderBy("duration_sec", "id")

gaps = (
    df_web_events
    .withColumn("is_new_session", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

islands = gaps.withColumn("burst_id", F.sum("is_new_session").over(w2))

deduped = (
    islands
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

valid = (
    deduped
    .filter(F.col("event_time") >= F.lit("2023-04-06 19:00:00").cast("timestamp"))
    .withColumn("rank_in_group_whole", F.floor("rank_in_group"))
    .withColumn("page_filled", F.coalesce("page", F.lit("OTHER")))
)

result = (
    valid
    .select(
        "*",
        F.avg("burst_id").over(w4).alias("burst_id_avg_3_prior"),
        F.lead("is_new_session", 1).over(w_running).alias("next_is_new_session"),
    )
)
