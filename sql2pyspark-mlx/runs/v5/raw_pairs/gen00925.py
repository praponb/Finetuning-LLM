from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page", "session_id").orderBy(F.col("duration_sec").desc(), "id")

deduped = (
    df_web_events
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .withColumn("quarter_start", F.date_trunc("QUARTER", "event_time"))
    .withColumn("duration_sec_tier", F.when(F.col("duration_sec") >= 508, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["quarter_start", "duration_sec_tier"], ["quarter_start"], []],
        "quarter_start",
        "duration_sec_tier",
    )
    .agg(
        F.count(F.when(F.col("event_type") == "search", 1)).alias("search_count"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("search_count") > 1)
)
