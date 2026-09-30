from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w2 = Window.partitionBy("session_id", "page").orderBy("duration_sec", "id")
w3 = Window.partitionBy("page", "event_type").orderBy("event_time", "id")
w4 = Window.partitionBy("event_type").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("session_id").orderBy("duration_sec", "id")
w6 = Window.partitionBy("page").orderBy("event_time", "id")
w7 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("page").orderBy("next_duration_sec", "id").rowsBetween(-3, Window.currentRow)
w9 = Window.partitionBy("page").orderBy(F.col("island_id").desc(), "id")
w10 = Window.partitionBy("page").orderBy("island_id_2", "id")
w11 = Window.partitionBy("page").orderBy(F.col("last4_avg_next_duration_sec_2").desc(), "id")

with_windows = (
    df_web_events
    .select(
        "id",
        "page",
        "event_time",
        F.avg("duration_sec").over(w).alias("duration_sec_avg_30d"),
        F.lead("duration_sec", 1).over(w2).alias("next_duration_sec"),
        F.lead("duration_sec", 1, 0).over(w3).alias("next_duration_sec_2"),
        F.sum("duration_sec").over(w4).alias("running_duration_sec"),
        F.row_number().over(w5).alias("seq_no"),
    )
)

flagged = (
    with_windows
    .withColumn("is_new_burst", F.when(F.lag("event_time").over(w6).isNull() | (F.col("event_time") > F.lag("event_time").over(w6) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("island_id", F.sum("is_new_burst").over(w7))

flagged_2 = (
    islands
    .withColumn("is_new_burst_2", F.when(F.col("event_time") - F.lag("event_time").over(w6) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessions_web_events = flagged_2.withColumn("island_id_2", F.sum("is_new_burst_2").over(w7))

metrics = (
    sessions_web_events
    .select(
        "id",
        "page",
        "next_duration_sec",
        "running_duration_sec",
        "island_id_2",
        F.avg("next_duration_sec_2").over(w_grp).alias("last4_avg_next_duration_sec_2"),
        F.sum("next_duration_sec_2").over(w_grp).alias("last4_sum_next_duration_sec_2"),
        F.count("next_duration_sec_2").over(w_grp).alias("last4_count_next_duration_sec_2"),
        F.row_number().over(w9).alias("row_seq"),
    )
)

scored = (
    metrics
    .withColumn("prev_next_duration_sec", F.lag("next_duration_sec", 2).over(w10))
    .withColumn("last4_avg_next_duration_sec_2_bucket", F.ntile(5).over(w11))
)

valid = (
    scored
    .filter(F.col("last4_avg_next_duration_sec_2") < 200)
    .select(
        "id",
        "page",
        "next_duration_sec",
        "island_id_2",
        "last4_sum_next_duration_sec_2",
        "last4_count_next_duration_sec_2",
        "row_seq",
        "prev_next_duration_sec",
        "last4_avg_next_duration_sec_2_bucket",
        F.coalesce("running_duration_sec", F.lit(0)).alias("running_duration_sec_filled"),
    )
)

base = (
    valid
    .filter(F.col("page").isNotNull())
    .select(
        "id",
        "page",
        "island_id_2",
        "last4_sum_next_duration_sec_2",
        "last4_count_next_duration_sec_2",
        "row_seq",
        "running_duration_sec_filled",
        F.ceil("prev_next_duration_sec").alias("prev_next_duration_sec_whole"),
        F.round((F.col("last4_sum_next_duration_sec_2") * 0.85), 2).alias("last4_sum_next_duration_sec_2_scaled"),
        F.coalesce("page", F.lit("NONE")).alias("resolved_page"),
    )
)

result = (
    base
    .groupBy("page")
    .agg(
        F.sum(F.when(F.col("page") == "/home", F.col("prev_next_duration_sec_whole")).otherwise(0)).alias("home_prev_next_duration_sec_whole"),
        F.sort_array(F.collect_set("page")).alias("page_list"),
        F.round(F.avg("row_seq"), 2).alias("avg_row_seq"),
    )
    .filter(F.col("home_prev_next_duration_sec_whole") > 0)
    .orderBy(F.col("avg_row_seq").desc_nulls_last())
)
