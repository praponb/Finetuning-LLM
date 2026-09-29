from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("browser").orderBy(F.col("prev_event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w4 = Window.orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.sum("duration_sec").alias("sum_web_event_duration_sec"))

prev_events = df_web_events.withColumn("prev_event_time", F.lag("event_time").over(w))

flagged = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_web_events = flagged.withColumn("session_no", F.sum("new_group_flag").over(w2))

joined = (
    sessions_web_events
    .alias("src")
    .join(df_sessions.alias("p"), F.col("src.session_id") == F.col("p.id"), "left")
    .filter(F.col("p.device").isin(["tablet", "mobile"]))
    .select("src.*", F.col("p.country"), F.col("p.browser"), F.col("p.device"))
)

kept_keys = (
    joined
    .select("id", "page")
    .subtract(
        df_web_events
        .filter(F.col("page").isin(["/cart", "/product"]))
        .select("id", "page")
    )
)

verified = (
    joined
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.page") == F.col("k.page")), "inner")
    .select("s.*")
)

metrics = verified.withColumn("new_group_flag_count_7d", F.count("new_group_flag").over(w3))

joined_web_events = (
    metrics
    .alias("src")
    .join(df_sessions.alias("b"), F.col("src.session_id") == F.col("b.id"), "left")
    .select(
        "src.session_id",
        "src.event_type",
        "src.event_time",
        "src.prev_event_time",
        "src.new_group_flag",
        "src.session_no",
        "src.country",
        "src.browser",
        "src.new_group_flag_count_7d",
        F.col("b.browser").alias("session_browser"),
    )
)

enriched = (
    joined_web_events
    .select(
        "session_id",
        "event_type",
        "event_time",
        "new_group_flag_count_7d",
        F.sum("new_group_flag_count_7d").over(w4).alias("new_group_flag_count_7d_sum_30d"),
    )
)

result = (
    enriched
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "session_id",
        "event_time",
        "new_group_flag_count_7d",
        "new_group_flag_count_7d_sum_30d",
        F.col("sum_web_event_duration_sec"),
    )
    .orderBy(F.col("new_group_flag_count_7d").desc_nulls_first())
)
