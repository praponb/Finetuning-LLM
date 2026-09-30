from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc_nulls_first())
w2 = Window.partitionBy("session_id").orderBy("event_time", "id")
w3 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_web_events
    .filter(F.col("page").isNotNull() & ~F.col("event_type").isin(["purchase", "add_to_cart", "click"]))
    .select(
        "*",
        F.concat_ws("/", "page", "event_type").alias("page_event_type_key"),
        F.initcap(F.trim("event_type")).alias("event_type_title"),
        F.coalesce("page", F.lit("OTHER")).alias("page_clean"),
    )
)

deduped = (
    valid
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

gaps = (
    deduped
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w2).isNull() | (F.col("event_time") > F.lag("event_time").over(w2) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_web_events = gaps.withColumn("session_no", F.sum("session_start_flag").over(w3))

summary = (
    sessions_web_events
    .withColumn("session_start_flag_tier", F.when(F.col("session_start_flag") >= 1, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["session_no", "session_start_flag_tier"], ["session_no"], []],
        "session_no",
        "session_start_flag_tier",
    )
    .agg(
        F.max("session_no").alias("max_session_no"),
        F.percentile_approx("session_start_flag", 0.95).alias("session_start_flag_p95"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.count(F.when(F.col("page") == "/home", 1)).alias("home_rows"),
        F.size(F.collect_set("page")).alias("num_pages"),
    )
    .filter(F.col("avg_duration_sec") < 0)
)

result = (
    summary
    .select(
        "session_no",
        "max_session_no",
        "session_start_flag_p95",
        "home_rows",
        "num_pages",
    )
    .orderBy(F.col("max_session_no").desc_nulls_first(), "num_pages")
)
