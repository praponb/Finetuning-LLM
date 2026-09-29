from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id", "page").orderBy("duration_sec", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id")
w3 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("page").orderBy("new_group_flag", "id")
w5 = Window.partitionBy("event_type", "page")
w6 = Window.partitionBy("page").orderBy(F.col("burst_id").desc())

base = (
    df_web_events
    .filter(F.col("page") == "/home")
    .withColumn("duration_sec_tier", F.when(F.col("duration_sec") >= 476, "High").when(F.col("duration_sec") >= 104, "Medium").otherwise("Low"))
)

latest = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

with_gaps = (
    latest
    .withColumn("new_group_flag", F.when(F.lag("event_time").over(w2).isNull() | (F.col("event_time") > F.lag("event_time").over(w2) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("burst_id", F.sum("new_group_flag").over(w3))

filtered = (
    sessionized
    .filter((F.col("event_time") >= F.lit("2023-07-14 21:00:00").cast("timestamp")) & F.col("page").isNotNull())
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        "new_group_flag",
        "burst_id",
        F.round("burst_id", 2).alias("burst_id_rounded"),
    )
)

with_windows = (
    filtered
    .withColumn("row_seq", F.row_number().over(w4))
    .withColumn("event_type_count_duration_sec", F.count("duration_sec").over(w5))
    .withColumn("burst_id_dense_rank", F.dense_rank().over(w6))
)

result = (
    with_windows
    .withColumn("duration_sec_tier", F.when(F.col("duration_sec") >= 423, "LARGE").otherwise("SMALL"))
    .withColumn("activity_month", F.date_trunc("MONTH", "event_time"))
    .groupingSets(
        [["duration_sec_tier", "activity_month", "page"], ["duration_sec_tier", "activity_month"]],
        "duration_sec_tier",
        "activity_month",
        "page",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("burst_id_rounded", 0.95).alias("burst_id_rounded_p95"),
        F.sum(F.when(F.col("page") == "/product", F.col("event_type_count_duration_sec"))).alias("product_total_event_type_count_duration_sec"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("product_total_event_type_count_duration_sec") < 1000000)
)
