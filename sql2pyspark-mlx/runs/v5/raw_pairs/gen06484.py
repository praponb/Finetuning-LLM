from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy("ends_on", "id")
w_running = Window.orderBy(F.col("ends_on").desc(), "id")
w5 = Window.orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.orderBy("last_known_event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_promotions
    .filter((F.col("starts_on") > F.to_date(F.lit("2023-07-03"))) & F.col("channel").isNotNull())
    .select(
        "*",
        F.col("discount_pct").cast("int").alias("discount_pct_int"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_clean"),
    )
)

metrics = (
    valid
    .select(
        "id",
        "ends_on",
        "channel_clean",
        F.first("channel", ignorenulls=True).over(w).alias("first_known_channel"),
        F.last("channel", ignorenulls=True).over(w2).alias("last_known_channel"),
    )
)

enriched = (
    metrics
    .select(
        "id",
        "ends_on",
        F.row_number().over(w_hist).alias("event_seq"),
        F.row_number().over(w_running).alias("row_seq"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "ends_on",
        "row_seq",
        F.lead("row_seq", 2).over(w_hist).alias("next_row_seq"),
        F.last("event_seq", ignorenulls=True).over(w5).alias("last_known_event_seq"),
    )
)

metrics_promotions = (
    with_windows
    .filter(F.col("next_row_seq") != 6)
    .select(
        "*",
        F.sum("last_known_event_seq").over(w_part).alias("running_sum_last_known_event_seq"),
        F.avg("last_known_event_seq").over(w_part).alias("running_avg_last_known_event_seq"),
        F.min("last_known_event_seq").over(w_part).alias("running_min_last_known_event_seq"),
        F.max("last_known_event_seq").over(w_part).alias("running_max_last_known_event_seq"),
    )
)

valid_promotions = (
    metrics_promotions
    .filter(F.col("ends_on") >= F.lit("2023-07-19").cast("date"))
    .select(
        "id",
        "ends_on",
        "row_seq",
        "next_row_seq",
        "last_known_event_seq",
        "running_avg_last_known_event_seq",
        "running_min_last_known_event_seq",
        "running_max_last_known_event_seq",
        F.col("last_known_event_seq").cast("double").alias("last_known_event_seq_str"),
        F.coalesce("last_known_event_seq", F.lit(0)).alias("last_known_event_seq_filled"),
    )
)

base = (
    valid_promotions
    .filter(F.col("ends_on") >= "2025-07-01")
    .select(
        "id",
        "ends_on",
        "row_seq",
        "next_row_seq",
        "last_known_event_seq",
        "running_avg_last_known_event_seq",
        "running_max_last_known_event_seq",
        "last_known_event_seq_str",
        "last_known_event_seq_filled",
        F.round((F.col("last_known_event_seq_str") / F.col("next_row_seq")), 2).alias("last_known_event_seq_str_per_next_row_seq"),
        (F.col("last_known_event_seq") + F.col("last_known_event_seq_str")).alias("last_known_event_seq_plus_last_known_event_seq_str"),
    )
)

result = (
    base
    .groupBy(
        F.date_trunc("YEAR", "ends_on").alias("year_start"),
        F.when(F.col("next_row_seq") >= 8, "LARGE").otherwise("SMALL").alias("next_row_seq_tier"),
        F.date_trunc("WEEK", "ends_on").alias("week_start"),
    )
    .agg(
        F.avg("row_seq").alias("avg_row_seq"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("event_count") < 1000)
)
