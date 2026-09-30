from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.orderBy(F.col("started_at_quarter").desc(), "id")
w3 = Window.partitionBy("browser").orderBy("country_max_started_at_quarter", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("country").orderBy("started_at_quarter", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("started_at_quarter").desc_nulls_first())
w6 = Window.partitionBy("browser").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w7 = Window.partitionBy("customer_id", "country").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w8 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w9 = Window.orderBy("country_max_started_at_quarter", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy("web_event_event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.orderBy("web_event_event_time", "id").rowsBetween(-2, Window.currentRow)
w12 = Window.orderBy("seq_no_avg_2", "id")
w13 = Window.orderBy(F.col("seq_no_avg_2").desc())

filtered = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-06-28 00:00:00").cast("timestamp")) & ~F.col("country").isin(["SG", "UK", "US"]))
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        F.quarter("started_at").alias("started_at_quarter"),
        F.substring("country", 1, 2).alias("country_prefix"),
        F.coalesce("country", F.lit("DIRECT")).alias("country_clean"),
    )
)

enriched = (
    filtered
    .select(
        "*",
        F.max("started_at_quarter").over(w).alias("country_max_started_at_quarter"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "started_at",
        "country_prefix",
        "country_clean",
        "country_max_started_at_quarter",
        "seq_no",
        F.avg("started_at_quarter").over(w3).alias("started_at_quarter_avg_5"),
        F.sum("country_max_started_at_quarter").over(w4).alias("running_country_max_started_at_quarter"),
        F.rank().over(w5).alias("started_at_quarter_rank"),
        F.sum("country_max_started_at_quarter").over(w6).alias("country_max_started_at_quarter_sum_30d"),
        F.max("country_max_started_at_quarter").over(w7).alias("country_max_started_at_quarter_max_7d"),
    )
)

base = (
    with_windows
    .filter(F.col("started_at_quarter_rank") >= 5)
    .withColumn("country_max_started_at_quarter_sum_30d_filled", F.coalesce("country_max_started_at_quarter_sum_30d", F.lit(0)))
)

with_last = (
    base
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.country_clean",
        "s.country_max_started_at_quarter",
        "s.seq_no",
        F.col("c.event_time").alias("web_event_event_time"),
    )
)

with_windows_sessions = (
    with_last
    .select(
        "*",
        F.first("seq_no", ignorenulls=True).over(w9).alias("first_known_seq_no"),
        F.sum("country_max_started_at_quarter").over(w10).alias("cumulative_country_max_started_at_quarter"),
        F.avg("seq_no").over(w11).alias("seq_no_avg_2"),
    )
)

with_windows_sessions_2 = (
    with_windows_sessions
    .withColumn("prev_first_known_seq_no", F.lag("first_known_seq_no").over(w12))
    .withColumn("next_seq_no_avg_2", F.lead("seq_no_avg_2").over(w12))
    .withColumn("prev_seq_no_avg_2", F.lag("seq_no_avg_2").over(w12))
    .withColumn("event_seq", F.row_number().over(w12))
    .withColumn("seq_no_avg_2_rank", F.rank().over(w13))
)

stats = (
    with_windows_sessions_2
    .groupBy(
        F.when(F.col("seq_no_avg_2_rank") >= 7, "PREMIUM").otherwise("STANDARD").alias("seq_no_avg_2_rank_tier"),
        F.date_trunc("WEEK", "web_event_event_time").alias("week_start"),
    )
    .agg(
        F.round(F.avg("seq_no"), 2).alias("avg_seq_no"),
        F.count("*").alias("row_count"),
        F.percentile_approx("first_known_seq_no", 0.95).alias("first_known_seq_no_p95"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("row_count") < 1000)
)

result = (
    stats
    .select(
        "seq_no_avg_2_rank_tier",
        "week_start",
        "row_count",
        "first_known_seq_no_p95",
    )
    .orderBy(
        F.col("row_count").desc_nulls_first(),
        F.col("first_known_seq_no_p95").desc(),
    )
)
