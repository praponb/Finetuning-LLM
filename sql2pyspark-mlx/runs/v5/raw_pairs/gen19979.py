from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time_hour", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id")
w3 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.orderBy(F.col("duration_sec").desc())
w5 = Window.partitionBy("device").orderBy(F.col("duration_sec").desc_nulls_first())
w6 = Window.partitionBy("country").orderBy(F.col("duration_sec").desc(), "id")
w_grp = Window.orderBy("duration_sec", "id").rowsBetween(-2, -1)

cleaned = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select(
        "*",
        F.hour("event_time").alias("event_time_hour"),
        F.coalesce("page", F.lit("N/A")).alias("page_filled"),
    )
)

latest = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        "event_time_hour",
    )
)

flagged = (
    latest
    .withColumn("new_group_flag", F.when(F.lag("event_time").over(w2).isNull() | (F.col("event_time") > F.lag("event_time").over(w2) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_web_events = flagged.withColumn("session_id_2", F.sum("new_group_flag").over(w3))

enriched = (
    sessions_web_events
    .alias("s")
    .join(df_sessions.alias("d"), F.col("s.session_id") == F.col("d.id"), "left")
    .select(
        "s.id",
        "s.session_id",
        "s.event_type",
        "s.duration_sec",
        F.col("d.country"),
        F.col("d.browser"),
        F.col("d.device"),
    )
)

base = (
    enriched
    .filter(F.col("duration_sec") > 280)
    .select(
        "id",
        "duration_sec",
        "country",
        "browser",
        "device",
        F.lower("device").alias("device_lower"),
        F.when(F.col("country").isin(["DE", "UK"]), "Priority").otherwise("Other").alias("country_group"),
    )
)

result = (
    base
    .withColumn("duration_sec_rank", F.rank().over(w_recent))
    .withColumn("duration_sec_percent_rank", F.percent_rank().over(w5))
    .withColumn("duration_sec_bucket", F.ntile(4).over(w6))
    .withColumn("duration_sec_avg_2_prior", F.avg("duration_sec").over(w_grp))
)
