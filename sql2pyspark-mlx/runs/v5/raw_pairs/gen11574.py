from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device").orderBy(F.col("started_at").desc())
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

filtered = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-04-27 14:00:00").cast("timestamp")) & (F.col("device") == "desktop"))
    .withColumn("browser_prefix", F.substring("browser", 1, 4))
    .withColumn("browser_lower", F.lower("browser"))
)

kept = (
    filtered
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "left_anti")
)

prev_events = kept.withColumn("prev_started_at", F.lag("started_at").over(w))

gaps = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_sessions = gaps.withColumn("island_id", F.sum("is_new_session").over(w2))

latest = (
    sessions_sessions
    .withColumn("dedup_rank", F.rank().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

latest_match = (
    latest
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_type").alias("web_event_event_type"))
)

cleaned = (
    latest_match
    .filter((F.col("country").eqNullSafe(F.lit("US"))) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "country",
        "browser_prefix",
        "browser_lower",
        "is_new_session",
        "island_id",
        "dedup_rank",
        F.when(F.col("device") == "tablet", 1).otherwise(0).alias("is_tablet"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "device",
        "browser",
        "country",
        "browser_prefix",
        "is_new_session",
        "dedup_rank",
        "is_tablet",
        F.when(F.col("device").rlike("^(tablet|mobile)$"), "OTHER_ZONE").when(F.col("device").rlike("^(desktop)$"), "EXTENDED").otherwise("GROUP_B").alias("device_zone"),
    )
    .orderBy(F.col("device").desc())
)
