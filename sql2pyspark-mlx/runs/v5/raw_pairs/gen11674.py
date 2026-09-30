from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("event_type").orderBy("duration_sec", "id")
w2 = Window.partitionBy("page").orderBy("days_since_event_time", "id")
w_ordered = Window.partitionBy("event_type", "page").orderBy("prev_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_web_events
    .filter(F.col("page") == "/product")
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.datediff(F.current_date(), "event_time").alias("days_since_event_time"),
        F.to_date("event_time").alias("event_time_date"),
    )
)

with_windows = (
    filtered
    .withColumn("prev_duration_sec", F.lag("duration_sec").over(w_grp))
    .withColumn("prev_days_since_event_time", F.lag("days_since_event_time").over(w_grp))
    .withColumn("next_days_since_event_time", F.lead("days_since_event_time").over(w_grp))
    .withColumn("row_seq", F.row_number().over(w_grp))
    .withColumn("prev_duration_sec_2", F.lag("duration_sec", 2).over(w2))
)

metrics = (
    with_windows
    .filter(F.col("row_seq") > 7)
    .withColumn("running_sum_days_since_event_time", F.sum("days_since_event_time").over(w_ordered))
    .withColumn("running_max_days_since_event_time", F.max("days_since_event_time").over(w_ordered))
    .withColumn("running_count_days_since_event_time", F.count("days_since_event_time").over(w_ordered))
)

joined = (
    metrics
    .alias("src")
    .join(df_sessions.alias("b"), F.col("src.session_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.browser"))
)

with_dims = (
    joined
    .alias("s")
    .join(df_sessions.alias("b"), F.col("s.session_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.browser").alias("session_browser"), F.col("b.country"))
)

result = (
    with_dims
    .withColumn("prev_duration_sec_2_tier", F.when(F.col("prev_duration_sec_2") >= 434, "PREMIUM").otherwise("STANDARD"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "event_time"))
    .groupingSets(
        [["prev_duration_sec_2_tier", "quarter_start"], ["prev_duration_sec_2_tier"]],
        "prev_duration_sec_2_tier",
        "quarter_start",
    )
    .agg(
        F.max("event_time").alias("last_seen"),
        F.count(F.when(F.col("country") == "US", 1)).alias("us_count"),
        F.round(F.sum("running_count_days_since_event_time") / F.nullif(F.count(F.when(F.col("country") == "UK", 1)), F.lit(0)), 2).alias("running_count_days_since_event_time_per_uk"),
        F.percentile_approx("row_seq", [0.25, 0.5, 0.75]).alias("row_seq_quartiles"),
        F.min("next_days_since_event_time").alias("min_next_days_since_event_time"),
    )
)
