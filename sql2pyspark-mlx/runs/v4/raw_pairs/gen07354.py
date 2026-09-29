from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy(F.col("starts_on_month_end").desc(), "id")

base = (
    df_promotions
    .filter((F.col("ends_on") >= F.lit("2024-01-17").cast("date")) & F.col("channel").isNotNull() & (F.col("promo_code") == "BUNDLE"))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        F.when(F.col("discount_pct") >= 0.36, "High").when(F.col("discount_pct") >= 0.17, "Medium").otherwise("Low").alias("discount_pct_band"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_clean"),
    )
)

cleaned = (
    base
    .filter(F.col("starts_on") >= F.lit("2023-10-12").cast("date"))
    .select(
        "id",
        "promo_code",
        "starts_on",
        "channel_clean",
        F.upper("promo_code").alias("promo_code_upper"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_filled"),
    )
)

filtered = (
    cleaned
    .filter((F.col("starts_on") > F.lit("2023-06-21").cast("date")) & F.col("promo_code").isin(["SPRING10", "WELCOME5", "VIP"]))
    .select(
        "id",
        "starts_on",
        "channel_clean",
        "channel_filled",
        F.last_day("starts_on").alias("starts_on_month_end"),
        F.add_months("starts_on", 1).alias("starts_on_plus_months"),
    )
)

with_windows = (
    filtered
    .select(
        "*",
        F.row_number().over(w_running).alias("event_seq"),
        F.row_number().over(w_running).alias("event_seq_2"),
    )
)

result = (
    with_windows
    .groupBy(
        F.date_trunc("month", "starts_on_plus_months").alias("activity_month"),
        F.date_trunc("week", "starts_on_month_end").alias("week_start"),
        F.when(F.col("event_seq_2") >= 9, "HIGH").otherwise("LOW").alias("event_seq_2_tier"),
    )
    .agg(
        F.round(F.avg("event_seq"), 2).alias("avg_event_seq"),
        F.percentile_approx("event_seq", 0.5).alias("event_seq_p50"),
        F.round(F.avg("event_seq_2"), 2).alias("avg_event_seq_2"),
        F.max("event_seq").alias("max_event_seq"),
        F.count("*").alias("num_rows"),
    )
)
