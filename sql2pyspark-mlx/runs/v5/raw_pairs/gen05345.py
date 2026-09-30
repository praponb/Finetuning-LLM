from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc())
w2 = Window.orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("duration_sec_filled", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("page", "event_type")

base = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select(
        "*",
        F.coalesce("duration_sec", F.lit(0)).alias("duration_sec_filled"),
        F.substring("page", 1, 3).alias("page_prefix"),
        F.coalesce("page", F.lit("N/A")).alias("page_clean"),
    )
)

ranked = (
    base
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

metrics = (
    ranked
    .filter(F.col("duration_sec_filled") < 33)
    .select(
        "id",
        "event_type",
        "duration_sec_filled",
        "page_clean",
        F.sum("duration_sec_filled").over(w2).alias("running_duration_sec_filled"),
        F.avg("duration_sec_filled").over(w3).alias("duration_sec_filled_avg_5"),
        F.stddev_samp("duration_sec_filled").over(w4).alias("page_stddev_samp_duration_sec_filled"),
    )
)

matched_keys = (
    metrics
    .select("id", "event_type")
    .exceptAll(
        df_web_events
        .filter(F.col("event_time") < "2024-03-01")
        .select("id", "event_type")
    )
)

kept_rows = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.event_type") == F.col("k.event_type")), "inner")
    .select("s.*")
)

cleaned = (
    kept_rows
    .filter(F.col("page_stddev_samp_duration_sec_filled") > 77)
    .select(
        "*",
        F.ceil("duration_sec_filled_avg_5").alias("duration_sec_filled_avg_5_whole"),
    )
)

result = (
    cleaned
    .groupBy("event_type")
    .agg(
        F.count(F.when(F.col("event_type") == "search", 1)).alias("search_rows"),
        F.round(F.sum("duration_sec_filled") / F.nullif(F.count(F.when(F.col("event_type") == "page_view", 1)), F.lit(0)), 2).alias("duration_sec_filled_per_page_view"),
        F.percentile_approx("duration_sec_filled", 0.5).alias("duration_sec_filled_p50"),
    )
)
