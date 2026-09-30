from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("country").orderBy("event_time_date", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time_date", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy("browser").orderBy(F.col("duration_sec").desc())
w4 = Window.partitionBy("page").orderBy("next_duration_sec", "id")

valid = (
    df_web_events
    .filter(F.col("event_time") >= "2024-06-01")
    .withColumn("event_time_date", F.to_date("event_time"))
)

with_dims = (
    valid
    .alias("e")
    .join(df_sessions.alias("s"), F.col("e.session_id") == F.col("s.id"), "left")
    .select("e.*", F.col("s.device"), F.col("s.browser"), F.col("s.country"))
)

scored = (
    with_dims
    .filter(F.col("event_type") == "add_to_cart")
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        "browser",
        F.lead("duration_sec").over(w_hist).alias("next_duration_sec"),
        F.lag("duration_sec").over(w_hist).alias("prev_duration_sec"),
        F.row_number().over(w_hist).alias("seq_no"),
        F.avg("duration_sec").over(w2).alias("duration_sec_avg_2_prior"),
    )
)

latest = (
    scored
    .withColumn("row_num", F.rank().over(w3))
    .filter(F.col("row_num") == 1)
)

deduped = (
    latest
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "event_type",
        "next_duration_sec",
        "prev_duration_sec",
        "seq_no",
        "duration_sec_avg_2_prior",
        "row_num",
    )
)

summary = (
    deduped
    .groupBy("event_type")
    .agg(
        F.size(F.collect_set("event_type")).alias("num_event_types"),
        F.count(F.when(F.col("event_type") == "click", 1)).alias("click_count"),
        F.percentile_approx("next_duration_sec", 0.9).alias("next_duration_sec_p90"),
        F.count(F.when(F.col("event_type") == "search", 1)).alias("search_rows"),
    )
)

result = (
    summary
    .select("event_type", "num_event_types", "next_duration_sec_p90", "search_rows")
)
