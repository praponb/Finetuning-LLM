from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type", "session_id").orderBy("duration_sec", "id")
w2 = Window.orderBy("duration_sec", "id").rowsBetween(-3, -1)
w_grp = Window.partitionBy("session_id").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w4 = Window.partitionBy("event_type").orderBy("event_time", "id")
w_ordered = Window.partitionBy("session_id", "page").orderBy("duration_sec", "id")
w6 = Window.partitionBy("session_browser").orderBy("next_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_web_events
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("duration_sec_avg_3_prior", F.avg("duration_sec").over(w2))
    .withColumn("duration_sec_sum_90d", F.sum("duration_sec").over(w_grp))
    .withColumn("next_duration_sec", F.lead("duration_sec", 2, 0).over(w4))
    .withColumn("duration_sec_bucket", F.ntile(5).over(w_ordered))
)

enriched = (
    scored
    .alias("e")
    .join(df_sessions.alias("s"), F.col("e.session_id") == F.col("s.id"), "left")
    .select(
        "e.id",
        "e.session_id",
        "e.duration_sec_avg_3_prior",
        "e.next_duration_sec",
        "e.duration_sec_bucket",
        F.col("s.browser"),
    )
)

with_dims = (
    enriched
    .alias("e")
    .join(df_sessions.alias("d"), F.col("e.session_id") == F.col("d.id"), "inner")
    .select(
        "e.id",
        "e.session_id",
        "e.next_duration_sec",
        F.col("d.browser").alias("session_browser"),
    )
)

valid = (
    with_dims
    .filter(F.col("session_browser").isNotNull() & (F.col("session_browser") == "Safari"))
    .select(
        "*",
        F.when(F.col("next_duration_sec") >= 452, "High").when(F.col("next_duration_sec") >= 94, "Medium").otherwise("Low").alias("next_duration_sec_level"),
    )
)

enriched_web_events = (
    valid
    .select(
        "id",
        "session_id",
        F.min("next_duration_sec").over(w6).alias("running_min_next_duration_sec"),
        F.sum("next_duration_sec").over(w6).alias("running_sum_next_duration_sec"),
        F.count("next_duration_sec").over(w6).alias("running_count_next_duration_sec"),
        F.avg("next_duration_sec").over(w6).alias("running_avg_next_duration_sec"),
    )
)

base = (
    enriched_web_events
    .filter(F.col("running_min_next_duration_sec") > 240)
    .select(
        "*",
        F.coalesce("running_count_next_duration_sec", F.lit(0)).alias("running_count_next_duration_sec_filled"),
        F.col("running_sum_next_duration_sec").cast("int").alias("running_sum_next_duration_sec_int"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("running_min_next_duration_sec") >= 410, "PREMIUM").otherwise("STANDARD").alias("running_min_next_duration_sec_tier"),
        F.when(F.col("running_count_next_duration_sec") >= 17, "PREMIUM").otherwise("STANDARD").alias("running_count_next_duration_sec_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("running_sum_next_duration_sec"), 2).alias("avg_running_sum_next_duration_sec"),
        F.percentile_approx("running_avg_next_duration_sec", 0.9).alias("running_avg_next_duration_sec_p90"),
    )
    .filter(F.col("running_avg_next_duration_sec_p90") < 1000000)
    .orderBy("running_avg_next_duration_sec_p90")
)
