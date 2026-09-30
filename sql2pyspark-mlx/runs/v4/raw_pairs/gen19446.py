from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("duration_sec_filled", "id")
w2 = Window.partitionBy("page").orderBy("duration_sec", "id")
w3 = Window.partitionBy("page", "session_id").orderBy("duration_sec", "id")

cleaned = (
    df_web_events
    .filter(F.col("event_time") >= F.lit("2023-05-07").cast("date"))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "duration_sec",
        F.to_date("event_time").alias("event_time_date"),
        F.coalesce("duration_sec", F.lit(0)).alias("duration_sec_filled"),
        F.coalesce("page", F.lit("N/A")).alias("resolved_page"),
    )
)

latest = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

ranked = (
    latest
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

deduped = (
    ranked
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .groupBy("session_id", "page")
    .pivot("event_type", ["add_to_cart", "search"])
    .agg(F.avg("duration_sec"))
)
