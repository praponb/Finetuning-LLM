from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("event_type").orderBy("island_id", "id")
w4 = Window.partitionBy("event_type").orderBy("page_length_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("event_type").orderBy(F.col("island_id_bucket").desc_nulls_first())
w6 = Window.orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w_running = Window.partitionBy("event_type").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("event_type").orderBy("island_id_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("event_type").orderBy("session_start_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("event_type").orderBy("island_id_bucket_percent_rank", "id")
w11 = Window.partitionBy("event_type").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)

filtered = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-07-08 21:00:00").cast("timestamp")) & F.col("page").isNotNull())
    .select(
        "id",
        "event_type",
        "event_time",
        "duration_sec",
        F.length("page").alias("page_length"),
        F.length("page").alias("page_length_2"),
    )
)

with_gaps = (
    filtered
    .withColumn("session_start_flag", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

islands = with_gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

with_windows = (
    islands
    .withColumn("island_id_bucket", F.ntile(3).over(w3))
    .withColumn("last_known_page_length_2", F.last("page_length_2", ignorenulls=True).over(w4))
)

metrics = (
    with_windows
    .select(
        "*",
        F.percent_rank().over(w5).alias("island_id_bucket_percent_rank"),
        F.avg("duration_sec").over(w6).alias("duration_sec_avg_7d"),
        F.first("session_start_flag", ignorenulls=True).over(w_running).alias("first_known_session_start_flag"),
        F.first("island_id_bucket", ignorenulls=True).over(w8).alias("first_known_island_id_bucket"),
        F.first("page_length_2", ignorenulls=True).over(w9).alias("first_known_page_length_2"),
    )
)

result = (
    metrics
    .select(
        "*",
        F.row_number().over(w10).alias("event_seq"),
        F.max("page_length").over(w11).alias("page_length_max_7d"),
    )
)
