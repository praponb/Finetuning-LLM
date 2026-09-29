from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("event_type", "page")
w4 = Window.partitionBy("event_type", "session_id").orderBy("duration_sec", "id")
w_running = Window.partitionBy("page").orderBy("session_id_2", "id")
w6 = Window.orderBy("prev_session_start_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("page").orderBy(F.col("prev_event_time").desc(), "id")
w8 = Window.partitionBy("page").orderBy(F.col("event_seq_minus_prev_session_start_flag").desc_nulls_last())
w9 = Window.partitionBy("page").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy("first_known_session_start_flag", "id")

prev_events = df_web_events.withColumn("prev_event_time", F.lag("event_time").over(w))

flagged = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("session_id_2", F.sum("session_start_flag").over(w2))

metrics = (
    islands
    .filter(F.col("event_time") >= "2023-04-01")
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "duration_sec",
        "prev_event_time",
        "session_start_flag",
        F.max("session_id_2").over(w3).alias("event_type_max_session_id_2"),
        F.lag("session_start_flag", 1).over(w4).alias("prev_session_start_flag"),
        F.lead("session_start_flag", 1).over(w_running).alias("next_session_start_flag"),
    )
)

enriched = (
    metrics
    .select(
        "id",
        "page",
        "duration_sec",
        "session_start_flag",
        "prev_session_start_flag",
        F.first("session_start_flag", ignorenulls=True).over(w6).alias("first_known_session_start_flag"),
        F.row_number().over(w7).alias("event_seq"),
    )
)

base = (
    enriched
    .filter(~F.col("page").eqNullSafe(F.lit("/cart")))
    .select(
        "*",
        F.substring("page", 1, 4).alias("page_prefix"),
        (F.col("event_seq") - F.col("prev_session_start_flag")).alias("event_seq_minus_prev_session_start_flag"),
        F.coalesce("page", F.lit("DIRECT")).alias("page_filled"),
    )
)

ranked = (
    base
    .withColumn("latest_rank", F.rank().over(w8))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "page",
        "first_known_session_start_flag",
        "event_seq",
        "page_prefix",
        "page_filled",
    )
)

with_windows = (
    ranked
    .withColumn("running_event_seq", F.sum("event_seq").over(w9))
    .withColumn("first_known_session_start_flag_bucket", F.ntile(4).over(w10))
)

valid = (
    with_windows
    .filter(F.col("page").isNotNull())
    .select(
        "*",
        F.when(F.col("running_event_seq") >= 41, "High").when(F.col("running_event_seq") >= 16, "Medium").otherwise("Low").alias("running_event_seq_tier"),
        F.when(F.col("first_known_session_start_flag") >= 1, "High").when(F.col("first_known_session_start_flag") >= 0, "Medium").otherwise("Low").alias("first_known_session_start_flag_band"),
        F.coalesce("page", F.lit("OTHER")).alias("page_filled_2"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("event_seq") >= 7, "PREMIUM").otherwise("STANDARD").alias("event_seq_tier"),
    )
    .agg(
        F.sum(F.when(F.col("page") == "/search", F.col("first_known_session_start_flag_bucket")).otherwise(0)).alias("search_first_known_session_start_flag_bucket"),
        F.max("first_known_session_start_flag_bucket").alias("max_first_known_session_start_flag_bucket"),
        F.countDistinct("id").alias("distinct_ids"),
        F.avg(F.when(F.col("page") == "/product", F.col("event_seq"))).alias("product_avg_event_seq"),
        F.max("first_known_session_start_flag").alias("max_first_known_session_start_flag"),
    )
    .filter(F.col("distinct_ids") >= 1)
    .orderBy(F.col("max_first_known_session_start_flag").asc_nulls_first())
)
