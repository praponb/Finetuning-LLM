from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("duration_sec", "id")
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.sum("duration_sec").alias("sum_web_event_duration_sec"))

filtered = (
    df_web_events
    .filter(F.col("page").isNotNull() & (F.col("event_type") != "purchase"))
    .select(
        "*",
        F.concat_ws("-", "event_type", "page").alias("event_type_page_key"),
        F.coalesce("page", F.lit("N/A")).alias("page_clean"),
    )
)

metrics = (
    filtered
    .withColumn("running_max_duration_sec", F.max("duration_sec").over(w))
    .withColumn("running_avg_duration_sec", F.avg("duration_sec").over(w))
    .withColumn("running_sum_duration_sec", F.sum("duration_sec").over(w))
    .withColumn("duration_sec_bucket", F.ntile(3).over(w2))
)

joined = (
    metrics
    .alias("src")
    .join(df_sessions.alias("s"), F.col("src.session_id") == F.col("s.id"), "left")
    .filter(F.col("s.started_at") < "2024-02-01")
    .select(
        "src.id",
        "src.session_id",
        "src.event_type",
        "src.page",
        "src.running_avg_duration_sec",
        "src.running_sum_duration_sec",
        "src.duration_sec_bucket",
        F.col("s.browser"),
        F.col("s.device"),
    )
)

enriched = (
    joined
    .alias("src")
    .join(df_sessions.alias("s"), F.col("src.session_id") == F.col("s.id"), "left")
    .select(
        "src.id",
        "src.session_id",
        "src.page",
        "src.running_avg_duration_sec",
        "src.browser",
        "src.device",
        F.col("s.country"),
    )
)

result = (
    enriched
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "page",
        "running_avg_duration_sec",
        "browser",
        "device",
        "country",
        F.when(F.col("device").rlike("^(tablet|desktop)$"), "GROUP_B").otherwise("PRIORITY").alias("device_zone"),
        F.col("sum_web_event_duration_sec"),
    )
)
