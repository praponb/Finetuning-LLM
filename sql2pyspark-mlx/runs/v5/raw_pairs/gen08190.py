from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.orderBy(F.col("started_at").desc(), "id")
w3 = Window.partitionBy("browser", "country").orderBy(F.col("started_at").desc(), "id")
w4 = Window.partitionBy("device").orderBy(F.col("event_seq").desc(), "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w6 = Window.orderBy(F.col("started_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w7 = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy(F.col("started_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)

with_windows = (
    df_sessions
    .filter(F.col("customer_id").isin([15, 23, 35]))
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("seq_no", F.row_number().over(w3))
)

latest = (
    with_windows
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("id", "started_at", "row_seq", "event_seq")
)

with_latest = (
    latest
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.duration_sec"))
)

not_flagged = (
    with_latest
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.to_date(F.col("ch.event_time")) >= "2024-05-15"), "left_anti")
)

enriched = (
    not_flagged
    .select(
        "id",
        "event_seq",
        F.count("event_seq").over(w6).alias("event_seq_count_30d"),
        F.last("row_seq", ignorenulls=True).over(w7).alias("last_known_row_seq"),
        F.max("row_seq").over(w8).alias("row_seq_max_7d"),
    )
)

grouped = (
    enriched
    .groupBy(
        F.when(F.col("event_seq") >= 8, "HIGH").otherwise("LOW").alias("event_seq_tier"),
    )
    .agg(
        F.avg("last_known_row_seq").alias("avg_last_known_row_seq"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("event_seq"), 2).alias("avg_event_seq"),
        F.percentile_approx("event_seq", [0.25, 0.5, 0.75]).alias("event_seq_quartiles"),
        F.percentile_approx("event_seq_count_30d", [0.25, 0.5, 0.75]).alias("event_seq_count_30d_quartiles"),
    )
)

result = (
    grouped
    .select(
        "event_seq_tier",
        "avg_last_known_row_seq",
        "avg_event_seq",
        "event_seq_quartiles",
        "event_seq_count_30d_quartiles",
    )
)
