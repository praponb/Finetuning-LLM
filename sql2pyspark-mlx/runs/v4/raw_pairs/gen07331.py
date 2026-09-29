from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("event_time_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w2 = Window.partitionBy("session_id").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.partitionBy("event_type").orderBy("event_time_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("session_id").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("session_id").orderBy(F.col("cumulative_duration_sec").desc_nulls_first(), "id")
w_ordered = Window.orderBy("duration_sec_count_7d", "id").rowsBetween(-2, -1)
w_grp = Window.partitionBy("session_id", "event_type").orderBy("duration_sec_count_7d", "id")
w8 = Window.partitionBy("event_type").orderBy("duration_sec", "id")
w9 = Window.partitionBy("event_type", "session_id").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w_grp_2 = Window.partitionBy("event_type", "session_id").orderBy(F.col("duration_sec").desc())

cleaned = (
    df_web_events
    .filter(F.col("event_time") >= F.lit("2023-08-21").cast("date"))
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        F.date_trunc("WEEK", "event_time").alias("event_time_period"),
        F.concat_ws("/", "event_type", "page").alias("event_type_page_key"),
    )
)

scored = (
    cleaned
    .select(
        "*",
        F.count("duration_sec").over(w).alias("duration_sec_count_7d"),
        F.max("duration_sec").over(w2).alias("duration_sec_max_30d"),
        F.sum("duration_sec").over(w3).alias("cumulative_duration_sec"),
        F.last("duration_sec", ignorenulls=True).over(w4).alias("last_known_duration_sec"),
    )
)

latest = (
    scored
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .select(
        "*",
        F.avg("cumulative_duration_sec").over(w_ordered).alias("cumulative_duration_sec_avg_2_prior"),
        F.ntile(10).over(w_grp).alias("duration_sec_count_7d_bucket"),
        F.row_number().over(w8).alias("row_seq"),
        F.sum("cumulative_duration_sec").over(w9).alias("cumulative_duration_sec_sum_7d"),
        F.rank().over(w_grp_2).alias("duration_sec_rank"),
    )
)
