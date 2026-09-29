from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("browser").orderBy("started_at", "id")
w3 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w_running = Window.partitionBy("event_type").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)

base = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-07-04 19:00:00").cast("timestamp")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        F.month("started_at").alias("started_at_month"),
        F.date_format("started_at", "yyyy-MM-dd").alias("started_at_label"),
    )
)

with_last = (
    base
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.duration_sec").alias("web_event_duration_sec"),
        F.col("c.event_type"),
    )
)

flagged = (
    with_last
    .withColumn("session_start_flag", F.when(F.col("started_at") - F.lag("started_at").over(w2) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

islands = flagged.withColumn("island_id", F.sum("session_start_flag").over(w3))

valid = (
    islands
    .filter(F.col("started_at") > F.to_date(F.lit("2024-01-12")))
    .select(
        "id",
        "customer_id",
        "started_at",
        "started_at_month",
        "started_at_label",
        "web_event_duration_sec",
        "event_type",
        "session_start_flag",
        F.quarter("started_at").alias("started_at_quarter"),
        F.coalesce("browser", F.lit("UNKNOWN")).alias("resolved_browser"),
    )
)

kept_keys = (
    valid
    .select("id", "customer_id")
    .subtract(
        df_sessions
        .filter(F.col("started_at") < "2024-01-08")
        .select("id", "customer_id")
    )
)

verified = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .select(
        "*",
        F.count("started_at_month").over(w4).alias("started_at_month_count_90d"),
        F.avg("started_at_month").over(w_running).alias("started_at_month_avg_7d"),
    )
)

filtered = (
    enriched
    .filter((F.col("started_at") >= F.to_date(F.lit("2023-10-19"))) & F.col("customer_id").isNotNull() & (F.col("started_at_month_avg_7d") != 59))
    .withColumn("started_at_label_2", F.date_format("started_at", "yyyy-MM"))
)

result = (
    filtered
    .groupBy(F.date_trunc("WEEK", "started_at").alias("week_start"))
    .agg(
        F.sum(F.when(F.col("event_type") == "search", F.col("started_at_month")).otherwise(0)).alias("search_started_at_month"),
        F.min("started_at").alias("first_started_at"),
        F.sum("session_start_flag").alias("total_session_start_flag"),
    )
)
