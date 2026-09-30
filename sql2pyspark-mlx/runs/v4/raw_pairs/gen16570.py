from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w_recent = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent_2 = Window.orderBy(F.col("duration_sec").desc(), "id")

base = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-12-24").cast("date")) & F.col("page").isNotNull())
    .select(
        "*",
        F.ceil("duration_sec").alias("duration_sec_whole"),
        F.coalesce("page", F.lit("OTHER")).alias("page_clean"),
    )
)

cleaned = (
    base
    .filter((F.col("event_time") > F.to_timestamp(F.lit("2023-09-12 15:00:00"))) & F.col("page").isNotNull())
    .withColumn("page_upper", F.upper("page"))
    .withColumn("days_since_event_time", F.datediff(F.current_date(), "event_time"))
    .withColumn("page_clean_2", F.coalesce("page", F.lit("NONE")))
)

scored = (
    cleaned
    .select(
        "*",
        F.sum("days_since_event_time").over(w).alias("days_since_event_time_sum_7d"),
        F.sum("duration_sec").over(w_recent).alias("cumulative_duration_sec"),
        F.ntile(4).over(w_recent_2).alias("duration_sec_bucket"),
    )
)

base_web_events = (
    scored
    .filter(F.col("event_time") > F.lit("2023-07-27 15:00:00").cast("timestamp"))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "duration_sec",
        "duration_sec_whole",
        "page_clean",
        "page_upper",
        "days_since_event_time",
        "page_clean_2",
        "days_since_event_time_sum_7d",
        "cumulative_duration_sec",
        "duration_sec_bucket",
        F.coalesce("duration_sec_bucket", F.lit(0)).alias("duration_sec_bucket_filled"),
        F.date_trunc("month", "event_time").alias("event_time_period"),
        F.coalesce("page", F.lit("DIRECT")).alias("resolved_page"),
    )
)

result = (
    base_web_events
    .groupBy(
        F.date_trunc("YEAR", "event_time_period").alias("year_start"),
        F.date_trunc("QUARTER", "event_time_period").alias("quarter_start"),
        F.date_trunc("WEEK", "event_time_period").alias("week_start"),
    )
    .agg(
        F.countDistinct("event_type").alias("distinct_event_types"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        F.count_if(F.col("event_type") == "page_view").alias("page_view_events"),
    )
)
