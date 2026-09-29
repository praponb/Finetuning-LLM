from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("session_id")
w4 = Window.partitionBy("page").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w_grp = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc_nulls_last())
w6 = Window.orderBy("session_start_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_web_events
    .withColumn("session_start_flag", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized = gaps.withColumn("burst_id", F.sum("session_start_flag").over(w2))

scored = (
    sessionized
    .select(
        "*",
        F.stddev_samp("duration_sec").over(w3).alias("session_id_stddev_samp_duration_sec"),
        F.max("burst_id").over(w4).alias("burst_id_max_90d"),
    )
)

cleaned = (
    scored
    .filter(F.col("event_time") > F.lit("2023-09-03").cast("date"))
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        "session_start_flag",
        "session_id_stddev_samp_duration_sec",
        F.when(F.col("duration_sec") >= 503, "High").when(F.col("duration_sec") >= 163, "Medium").otherwise("Low").alias("duration_sec_level"),
        F.col("session_start_flag").cast("string").alias("session_start_flag_str"),
    )
)

scored_web_events = (
    cleaned
    .select(
        "*",
        F.rank().over(w_grp).alias("duration_sec_rank"),
        F.first("session_id_stddev_samp_duration_sec", ignorenulls=True).over(w6).alias("first_known_session_id_stddev_samp_duration_sec"),
    )
)

result = (
    scored_web_events
    .withColumn("quarter_start", F.date_trunc("QUARTER", "event_time"))
    .withColumn("week_start", F.date_trunc("WEEK", "event_time"))
    .groupingSets(
        [["quarter_start", "week_start"], ["quarter_start"], []],
        "quarter_start",
        "week_start",
    )
    .agg(
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("span_minutes"),
        F.sum(F.when(F.col("event_type") == "click", F.col("session_start_flag")).otherwise(0)).alias("click_session_start_flag"),
        F.avg("duration_sec_rank").alias("avg_duration_sec_rank"),
        F.min("event_time").alias("started_at"),
    )
    .orderBy(F.col("quarter_start").desc())
)
