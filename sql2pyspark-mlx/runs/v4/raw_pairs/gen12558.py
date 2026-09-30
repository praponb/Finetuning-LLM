from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc(), "id")
w2 = Window.partitionBy("page").orderBy(F.col("duration_sec").desc(), "id")
w_ordered = Window.partitionBy("page", "event_type").orderBy("duration_sec", "id")
w4 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("session_id", "event_type").orderBy("duration_sec_bucket", "id")
w_grp = Window.partitionBy("event_type")
w7 = Window.partitionBy("session_id", "event_type").orderBy(F.col("event_type_sum_duration_sec").asc_nulls_first())
w8 = Window.partitionBy("event_type").orderBy("rn", "id").rowsBetween(-2, -1)
w9 = Window.partitionBy("session_id", "event_type").orderBy(F.col("last_known_duration_sec_bucket").desc(), "id")

scored = (
    df_web_events
    .select(
        "*",
        F.ntile(3).over(w).alias("duration_sec_bucket"),
        F.ntile(3).over(w2).alias("duration_sec_bucket_2"),
    )
)

scored_web_events = (
    scored
    .withColumn("next_duration_sec", F.lead("duration_sec", 1, 0).over(w_ordered))
    .withColumn("last_known_duration_sec_bucket", F.last("duration_sec_bucket", ignorenulls=True).over(w4))
    .withColumn("prev_duration_sec_bucket_2", F.lag("duration_sec_bucket_2", 1).over(w5))
    .withColumn("event_type_sum_duration_sec", F.sum("duration_sec").over(w_grp))
)

latest = (
    scored_web_events
    .withColumn("rn", F.rank().over(w7))
    .filter(F.col("rn") == 1)
)

filtered = (
    latest
    .filter(F.col("event_time") >= F.to_timestamp(F.lit("2024-02-13 14:00:00")))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "duration_sec",
        "duration_sec_bucket",
        "next_duration_sec",
        "last_known_duration_sec_bucket",
        "prev_duration_sec_bucket_2",
        "rn",
        F.when(F.col("prev_duration_sec_bucket_2") >= 7, "High").when(F.col("prev_duration_sec_bucket_2") >= 4, "Medium").otherwise("Low").alias("prev_duration_sec_bucket_2_band"),
        F.coalesce("page", F.lit("UNKNOWN")).alias("page_filled"),
    )
)

result = (
    filtered
    .select(
        "*",
        F.avg("next_duration_sec").over(w8).alias("next_duration_sec_avg_2_prior"),
        F.row_number().over(w9).alias("event_seq"),
    )
)
