from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("event_type").orderBy(F.col("is_new_session").desc_nulls_first(), "id")
w4 = Window.partitionBy("page").orderBy("event_time", "id")
w5 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("is_new_session", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_web_events
    .filter(F.col("duration_sec").isNull())
    .withColumn("page_prefix", F.substring("page", 1, 4))
    .withColumn("page_filled", F.coalesce("page", F.lit("N/A")))
)

with_gaps = (
    base
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("burst_id", F.sum("is_new_session").over(w2))

deduped = (
    islands
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

cleaned = (
    deduped
    .filter((F.col("event_time") > F.to_date(F.lit("2023-06-04"))) & (F.col("rank_in_group") > 1))
    .select(
        "id",
        "page",
        "event_time",
        "duration_sec",
        "page_prefix",
        "page_filled",
        "is_new_session",
        "burst_id",
        "rank_in_group",
        F.col("event_time").cast("date").alias("event_time_day"),
    )
)

with_gaps_2 = (
    cleaned
    .withColumn("is_new_burst", F.when(F.col("event_time") - F.lag("event_time").over(w4) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessions_web_events = with_gaps_2.withColumn("burst_id_2", F.sum("is_new_burst").over(w5))

metrics = (
    sessions_web_events
    .withColumn("next_burst_id_2", F.lead("burst_id_2").over(w4))
    .withColumn("prev_burst_id_2", F.lag("burst_id_2").over(w4))
    .withColumn("prev_is_new_session", F.lag("is_new_session").over(w4))
    .withColumn("next_is_new_session", F.lead("is_new_session").over(w4))
    .withColumn("row_seq", F.row_number().over(w4))
    .withColumn("cumulative_burst_id", F.sum("burst_id").over(w6))
)

flagged = (
    metrics
    .withColumn("is_new_session_2", F.when(F.lag("event_time").over(w4).isNull() | (F.col("event_time") > F.lag("event_time").over(w4) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands_web_events = flagged.withColumn("burst_id_3", F.sum("is_new_session_2").over(w5))

result = (
    islands_web_events
    .cube(
        "cumulative_burst_id",
        F.when(F.col("is_new_burst") >= 1, "HIGH").otherwise("LOW").alias("is_new_burst_tier"),
        F.date_trunc("MONTH", "event_time").alias("activity_month"),
    )
    .agg(
        F.count(F.when(F.col("page") == "/home", 1)).alias("home_rows"),
        F.size(F.collect_set("page")).alias("num_pages"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("duration_min"),
        F.count("*").alias("event_count"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("duration_min") > 0)
    .orderBy(F.col("num_pages").asc_nulls_first())
)
