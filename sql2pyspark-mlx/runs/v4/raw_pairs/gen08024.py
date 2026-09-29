from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("event_type").orderBy("event_time_weekofyear", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("page").orderBy(F.col("event_time_weekofyear").desc_nulls_last())

valid = (
    df_web_events
    .filter(F.col("event_type").isin(["search", "add_to_cart", "purchase"]))
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.weekofyear("event_time").alias("event_time_weekofyear"),
    )
)

scored = (
    valid
    .withColumn("next_duration_sec", F.lead("duration_sec", 1, 0).over(w))
    .withColumn("last_known_duration_sec", F.last("duration_sec", ignorenulls=True).over(w2))
    .withColumn("running_event_time_weekofyear", F.sum("event_time_weekofyear").over(w3))
)

with_prev = scored.withColumn("prev_event_time", F.lag("event_time").over(w))

flagged = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("session_id", F.sum("session_start_flag").over(w2))

with_gaps = (
    islands
    .withColumn("new_group_flag", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

islands_web_events = with_gaps.withColumn("burst_id", F.sum("new_group_flag").over(w2))

latest = (
    islands_web_events
    .withColumn("row_num", F.rank().over(w4))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .withColumn("week_start", F.date_trunc("WEEK", "event_time"))
    .groupingSets(
        [["week_start", "session_id"], ["week_start"], []],
        "week_start",
        "session_id",
    )
    .agg(
        F.count(F.when(F.col("page") == "/product", 1)).alias("product_count"),
        F.count(F.when(F.col("event_type") == "click", 1)).alias("click_count"),
        F.percentile_approx("duration_sec", 0.95).alias("duration_sec_p95"),
    )
    .filter(F.col("product_count") < 1)
)
