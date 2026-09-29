from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("days_since_event_time", "id")
w_grp = Window.partitionBy("event_type").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.partitionBy("event_type", "session_id").orderBy("row_num", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc())
w5 = Window.orderBy(F.col("duration_sec").asc_nulls_last())
w_ordered = Window.partitionBy("session_id", "page").orderBy(F.col("duration_sec").desc_nulls_last())
w7 = Window.orderBy("event_time", "id")
w_part = Window.orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("first_known_duration_sec_avg_30d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy("first_known_duration_sec_avg_30d", "id").rowsBetween(-3, Window.currentRow)
w11 = Window.orderBy("prev_duration_sec_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.orderBy(F.col("duration_sec_rank").desc())

filtered = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select(
        "*",
        F.datediff(F.current_date(), "event_time").alias("days_since_event_time"),
        F.coalesce("page", F.lit("NONE")).alias("resolved_page"),
    )
)

ranked = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

scored = (
    ranked
    .select(
        "id",
        "event_time",
        "days_since_event_time",
        F.avg("duration_sec").over(w_grp).alias("duration_sec_avg_30d"),
        F.last("days_since_event_time", ignorenulls=True).over(w3).alias("last_known_days_since_event_time"),
        F.cume_dist().over(w4).alias("duration_sec_cume_dist"),
        F.rank().over(w5).alias("duration_sec_rank"),
        F.percent_rank().over(w_ordered).alias("duration_sec_percent_rank"),
    )
)

scored_web_events = (
    scored
    .select(
        "*",
        F.lag("duration_sec_percent_rank", 1).over(w7).alias("prev_duration_sec_percent_rank"),
        F.first("duration_sec_avg_30d", ignorenulls=True).over(w_part).alias("first_known_duration_sec_avg_30d"),
    )
)

base = (
    scored_web_events
    .filter(F.col("days_since_event_time").between(33, 58))
    .select(
        "id",
        "days_since_event_time",
        "duration_sec_rank",
        "duration_sec_percent_rank",
        "prev_duration_sec_percent_rank",
        "first_known_duration_sec_avg_30d",
        F.when(F.col("duration_sec_avg_30d") >= 367, "High").when(F.col("duration_sec_avg_30d") >= 82, "Medium").otherwise("Low").alias("duration_sec_avg_30d_band"),
    )
)

metrics = (
    base
    .select(
        "*",
        F.sum("duration_sec_percent_rank").over(w9).alias("cumulative_duration_sec_percent_rank"),
        F.avg("days_since_event_time").over(w10).alias("days_since_event_time_avg_3"),
        F.sum("duration_sec_rank").over(w11).alias("cumulative_duration_sec_rank"),
        F.dense_rank().over(w12).alias("duration_sec_rank_dense_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "days_since_event_time",
        "duration_sec_rank",
        "first_known_duration_sec_avg_30d",
        "duration_sec_avg_30d_band",
        "cumulative_duration_sec_percent_rank",
        "cumulative_duration_sec_rank",
        "duration_sec_rank_dense_rank",
    )
)
