from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent_2 = Window.partitionBy("page").orderBy("duration_sec_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("page").orderBy("duration_sec", "id")
w4 = Window.partitionBy("page").orderBy(F.col("days_since_event_time").desc())
w5 = Window.partitionBy("page").orderBy(F.col("days_since_event_time").desc(), "id")
w6 = Window.partitionBy("page").orderBy("running_duration_sec_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_web_events
    .filter(F.col("duration_sec").between(286, 437))
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.floor("duration_sec").alias("duration_sec_whole"),
        F.coalesce("page", F.lit("UNKNOWN")).alias("page_filled"),
    )
)

scored = (
    cleaned
    .select(
        "id",
        "page",
        "event_time",
        "duration_sec",
        "duration_sec_whole",
        "page_filled",
        F.sum("duration_sec_whole").over(w_recent).alias("running_duration_sec_whole"),
        F.last("duration_sec_whole", ignorenulls=True).over(w_recent_2).alias("last_known_duration_sec_whole"),
    )
)

base = (
    scored
    .filter(F.col("event_time") > F.lit("2023-06-16 10:00:00").cast("timestamp"))
    .select(
        "*",
        F.upper("page").alias("page_upper"),
        F.coalesce("page", F.lit("OTHER")).alias("page_clean"),
    )
)

valid = (
    base
    .filter((F.col("event_time") >= F.lit("2023-10-23 13:00:00").cast("timestamp")) & F.col("page").isNotNull())
    .select(
        "*",
        F.datediff(F.current_date(), "event_time").alias("days_since_event_time"),
        F.coalesce("page", F.lit("UNKNOWN")).alias("resolved_page"),
    )
)

filtered = (
    valid
    .filter(F.col("running_duration_sec_whole").isNull())
    .select(
        "*",
        F.when(F.col("running_duration_sec_whole") >= 345, "High").when(F.col("running_duration_sec_whole") >= 88, "Medium").otherwise("Low").alias("running_duration_sec_whole_tier"),
    )
)

enriched = (
    filtered
    .select(
        "*",
        F.ntile(10).over(w3).alias("duration_sec_bucket"),
        F.cume_dist().over(w4).alias("days_since_event_time_cume_dist"),
        F.ntile(10).over(w5).alias("days_since_event_time_bucket"),
        F.first("duration_sec_whole", ignorenulls=True).over(w6).alias("first_known_duration_sec_whole"),
    )
)

result = (
    enriched
    .groupBy(
        F.date_trunc("WEEK", "event_time").alias("week_start"),
        F.date_trunc("QUARTER", "event_time").alias("quarter_start"),
        F.when(F.col("duration_sec_bucket") >= 8, "HIGH").otherwise("LOW").alias("duration_sec_bucket_tier"),
    )
    .agg(
        F.count(F.when(F.col("page") == "/search", 1)).alias("search_rows"),
        F.bit_and("duration_sec").alias("duration_sec_bit_and"),
        F.count_if(F.col("page") == "/search").alias("search_events"),
        F.bit_and("duration_sec").alias("duration_sec_bit_and_2"),
    )
)
