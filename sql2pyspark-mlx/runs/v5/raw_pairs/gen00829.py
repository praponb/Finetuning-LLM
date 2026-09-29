from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("browser").orderBy("web_event_event_time", "id")
w3 = Window.partitionBy("browser").orderBy("web_event_event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("browser", "country").orderBy(F.col("session_id").desc(), "id")
w5 = Window.partitionBy("customer_id").orderBy("session_start_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_sessions
    .filter(F.col("customer_id").isNull())
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        F.when(F.col("country") == "US", 1).otherwise(0).alias("is_us"),
        F.coalesce("country", F.lit("UNKNOWN")).alias("country_clean"),
    )
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.event_time").alias("web_event_event_time"),
        F.col("c.event_type"),
    )
)

gaps = (
    with_latest
    .withColumn("session_start_flag", F.when(F.lag("web_event_event_time").over(w2).isNull() | (F.col("web_event_event_time") > F.lag("web_event_event_time").over(w2) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_sessions = gaps.withColumn("session_id", F.sum("session_start_flag").over(w3))

enriched = (
    sessions_sessions
    .select(
        "id",
        "browser",
        "started_at",
        "country",
        "web_event_event_time",
        "event_type",
        "session_start_flag",
        "session_id",
        F.ntile(5).over(w4).alias("session_id_bucket"),
        F.sum("session_start_flag").over(w5).alias("running_session_start_flag"),
    )
)

scored = (
    enriched
    .filter(F.col("running_session_start_flag").between(2, 4))
    .withColumn("next_session_start_flag", F.lead("session_start_flag").over(w2))
    .withColumn("next_running_session_start_flag", F.lead("running_session_start_flag").over(w2))
    .withColumn("event_seq", F.row_number().over(w2))
)

result = (
    scored
    .select(
        "id",
        "browser",
        "started_at",
        "country",
        "event_type",
        "session_start_flag",
        "session_id",
        "session_id_bucket",
        "running_session_start_flag",
    )
    .orderBy(F.col("session_id").desc_nulls_last())
)
