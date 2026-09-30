from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("duration_sec_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("session_id").orderBy("running_sum_duration_sec_rounded", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("session_id").orderBy("running_duration_sec_rounded", "id")

valid = (
    df_web_events
    .filter(F.col("session_id").isin([16, 37]))
    .withColumn("duration_sec_rounded", F.round("duration_sec", 1))
    .withColumn("is_checkout", F.when(F.col("page") == "/checkout", 1).otherwise(0))
    .withColumn("page_clean", F.coalesce("page", F.lit("UNKNOWN")))
)

base = (
    valid
    .filter(F.col("page").isNotNull() & (F.col("event_time") > F.lit("2023-05-16 22:00:00").cast("timestamp")))
    .select(
        "id",
        "session_id",
        "duration_sec",
        "duration_sec_rounded",
        F.col("duration_sec_rounded").cast("int").alias("duration_sec_rounded_int"),
        F.col("duration_sec").cast("string").alias("duration_sec_num"),
        F.coalesce("page", F.lit("OTHER")).alias("page_filled"),
    )
)

with_windows = (
    base
    .select(
        "id",
        "session_id",
        "duration_sec",
        "duration_sec_num",
        "page_filled",
        F.sum("duration_sec_rounded").over(w).alias("running_sum_duration_sec_rounded"),
        F.avg("duration_sec_rounded").over(w).alias("running_avg_duration_sec_rounded"),
        F.max("duration_sec_rounded").over(w).alias("running_max_duration_sec_rounded"),
        F.sum("duration_sec_rounded").over(w).alias("running_duration_sec_rounded"),
    )
)

scored = (
    with_windows
    .filter(F.col("running_avg_duration_sec_rounded") >= 68)
    .withColumn("last3_sum_running_duration_sec_rounded", F.sum("running_duration_sec_rounded").over(w2))
    .withColumn("last3_avg_running_duration_sec_rounded", F.avg("running_duration_sec_rounded").over(w2))
    .withColumn("last3_min_running_duration_sec_rounded", F.min("running_duration_sec_rounded").over(w2))
    .withColumn("last3_count_running_duration_sec_rounded", F.count("running_duration_sec_rounded").over(w2))
    .withColumn("running_duration_sec_rounded_bucket", F.ntile(4).over(w3))
)

result = (
    scored
    .select(
        "id",
        "session_id",
        "duration_sec_num",
        "running_sum_duration_sec_rounded",
        "running_avg_duration_sec_rounded",
        "running_max_duration_sec_rounded",
        "running_duration_sec_rounded",
        "last3_min_running_duration_sec_rounded",
        "running_duration_sec_rounded_bucket",
    )
)
