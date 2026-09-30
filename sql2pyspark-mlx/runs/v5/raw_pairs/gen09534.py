from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.orderBy(F.col("last3_count_duration_sec").desc())
w3 = Window.partitionBy("page").orderBy("last3_sum_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("page", "event_type").orderBy("last3_count_duration_sec", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("event_type").orderBy("last3_count_duration_sec", "id")

scored = (
    df_web_events
    .filter(F.col("page") == "/checkout")
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        F.max("duration_sec").over(w_running).alias("last3_max_duration_sec"),
        F.avg("duration_sec").over(w_running).alias("last3_avg_duration_sec"),
        F.count("duration_sec").over(w_running).alias("last3_count_duration_sec"),
        F.sum("duration_sec").over(w_running).alias("last3_sum_duration_sec"),
    )
)

cleaned = (
    scored
    .filter(F.col("last3_sum_duration_sec") >= 2000)
    .select(
        "id",
        "event_type",
        "page",
        "last3_max_duration_sec",
        "last3_avg_duration_sec",
        "last3_count_duration_sec",
        "last3_sum_duration_sec",
        F.when(F.col("last3_avg_duration_sec") >= 420, "High").when(F.col("last3_avg_duration_sec") >= 210, "Medium").otherwise("Low").alias("last3_avg_duration_sec_level"),
        F.col("last3_sum_duration_sec").cast("int").alias("last3_sum_duration_sec_int"),
        F.coalesce("page", F.lit("UNKNOWN")).alias("page_filled"),
    )
)

with_windows = (
    cleaned
    .select(
        "id",
        "event_type",
        "page",
        "last3_count_duration_sec",
        "last3_avg_duration_sec_level",
        "page_filled",
        F.percent_rank().over(w2).alias("last3_count_duration_sec_percent_rank"),
        F.sum("last3_sum_duration_sec").over(w3).alias("cumulative_last3_sum_duration_sec"),
        F.avg("last3_sum_duration_sec").over(w4).alias("last3_sum_duration_sec_avg_2"),
    )
)

deduped = (
    with_windows
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .select(
        "id",
        "event_type",
        "last3_count_duration_sec",
        "last3_avg_duration_sec_level",
        "last3_count_duration_sec_percent_rank",
        "cumulative_last3_sum_duration_sec",
        "latest_rank",
    )
    .orderBy(F.col("last3_count_duration_sec_percent_rank").desc_nulls_last())
)
