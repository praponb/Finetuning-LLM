from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("event_type").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w4 = Window.partitionBy("event_type", "session_id")
w5 = Window.partitionBy("session_id").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w6 = Window.partitionBy("session_id").orderBy(F.col("duration_sec").asc_nulls_first())
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.sum("duration_sec").alias("sum_web_event_duration_sec"))

cleaned = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-04-29 10:00:00").cast("timestamp")) & F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        F.concat("page", F.lit(", "), "event_type").alias("page_event_type"),
        F.lower("event_type").alias("event_type_lower"),
    )
)

with_gaps = (
    cleaned
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

remaining_keys = (
    islands
    .select("session_id", "id")
    .intersect(
        df_web_events
        .filter(F.col("duration_sec").between(147, 372))
        .select("session_id", "id")
    )
)

kept_rows = (
    islands
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.session_id") == F.col("k.session_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .withColumn("session_no_avg_30d", F.avg("session_no").over(w3))
    .withColumn("event_type_min_duration_sec", F.min("duration_sec").over(w4))
    .withColumn("duration_sec_sum_90d", F.sum("duration_sec").over(w5))
    .withColumn("duration_sec_cume_dist", F.cume_dist().over(w6))
)

base = (
    enriched
    .filter(F.col("session_no_avg_30d") <= 2)
    .select("*", F.round("duration_sec", 0).alias("duration_sec_rounded"))
)

with_dims = (
    base
    .alias("src")
    .join(df_sessions.alias("b"), F.col("src.session_id") == F.col("b.id"), "inner")
    .filter(F.col("b.browser") == "Firefox")
    .select(
        "src.session_id",
        "src.event_type",
        "src.event_time",
        "src.duration_sec",
        "src.page_event_type",
        "src.event_type_lower",
        "src.session_start_flag",
        "src.session_no",
        "src.duration_sec_sum_90d",
        "src.duration_sec_cume_dist",
        "src.duration_sec_rounded",
        F.col("b.device"),
        F.col("b.country"),
    )
)

pivoted = (
    with_dims
    .groupBy("session_id")
    .pivot("event_type", ["purchase", "page_view"])
    .agg(F.max("duration_sec_sum_90d").alias("amount"), F.count(F.lit(1)).alias("n"))
)

result = (
    pivoted
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "session_id",
        "purchase_amount",
        "page_view_amount",
        "page_view_n",
        F.col("sum_web_event_duration_sec"),
    )
    .orderBy(F.col("page_view_n").desc_nulls_last())
)
