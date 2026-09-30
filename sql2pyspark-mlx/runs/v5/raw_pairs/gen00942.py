from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("device", "browser").orderBy("island_id", "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("event_type").orderBy("event_time", "id")
w6 = Window.partitionBy("event_type").orderBy("prev_island_id", "id")

base = (
    df_sessions
    .filter(F.col("browser").isNotNull() & (F.col("browser").eqNullSafe(F.lit("Firefox"))))
    .select("*", F.initcap(F.trim("device")).alias("device_title"))
)

gaps = (
    base
    .withColumn("is_new_burst", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        F.sum("is_new_burst").over(w2).alias("session_id"),
    )
)

flagged = (
    sessionized
    .withColumn("session_start_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "session_id",
        F.sum("session_start_flag").over(w2).alias("island_id"),
    )
)

scored = (
    islands
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "island_id",
        F.lag("island_id").over(w_running).alias("prev_island_id"),
        F.lag("session_id").over(w_running).alias("prev_session_id"),
        F.lead("session_id").over(w_running).alias("next_session_id"),
        F.row_number().over(w_running).alias("row_seq"),
    )
)

with_latest = (
    scored
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.prev_island_id",
        "s.prev_session_id",
        "s.next_session_id",
        F.col("c.event_type"),
        F.col("c.event_time"),
    )
)

enriched = (
    with_latest
    .select(
        "*",
        F.lead("next_session_id").over(w5).alias("next_next_session_id"),
        F.lag("prev_session_id").over(w5).alias("prev_prev_session_id"),
        F.lead("prev_session_id").over(w5).alias("next_prev_session_id"),
        F.row_number().over(w5).alias("seq_no"),
        F.lead("prev_session_id", 1).over(w6).alias("next_prev_session_id_2"),
    )
)

result = (
    enriched
    .withColumn("activity_month", F.date_trunc("MONTH", "event_time"))
    .cube("next_session_id", "activity_month")
    .agg(
        F.round(F.sum("prev_island_id") / F.nullif(F.count(F.when(F.col("event_type") == "add_to_cart", 1)), F.lit(0)), 2).alias("prev_island_id_per_add_to_cart"),
        F.min("event_time").alias("first_seen"),
        F.min("event_time").alias("started_at"),
        F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchase_count"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("duration_min"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("duration_min").desc_nulls_first())
)
