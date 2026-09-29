from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("discount_pct", "id").rowsBetween(-3, -1)
w_hist = Window.partitionBy("channel").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("promo_code").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("promo_code").orderBy("ends_on", "id")
w_part = Window.partitionBy("promo_code").orderBy("starts_on", "id").rowsBetween(-2, -1)
w_ordered = Window.orderBy("event_seq_rounded", "id")
w7 = Window.orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_promotions
    .filter((F.col("starts_on") > F.lit("2023-04-22").cast("date")) & F.col("channel").isNotNull())
    .select(
        "*",
        F.last_day("starts_on").alias("starts_on_month_end"),
        F.coalesce("channel", F.lit("DIRECT")).alias("resolved_channel"),
    )
)

with_windows = (
    filtered
    .filter(F.col("discount_pct") >= 0.21)
    .select(
        "id",
        "channel",
        "discount_pct",
        "resolved_channel",
        F.avg("discount_pct").over(w).alias("discount_pct_avg_3_prior"),
        F.first("channel", ignorenulls=True).over(w_hist).alias("first_known_channel"),
        F.first("channel", ignorenulls=True).over(w3).alias("first_known_channel_2"),
        F.row_number().over(w4).alias("event_seq"),
        F.avg("discount_pct").over(w_part).alias("discount_pct_avg_2_prior"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("discount_pct") < 0.23)
    .select(
        "id",
        "discount_pct",
        "resolved_channel",
        "first_known_channel",
        "first_known_channel_2",
        "event_seq",
        F.round("event_seq", 0).alias("event_seq_rounded"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_clean"),
    )
)

metrics = (
    cleaned
    .select(
        "id",
        "resolved_channel",
        "first_known_channel_2",
        "event_seq",
        "event_seq_rounded",
        F.row_number().over(w_ordered).alias("row_seq"),
        F.last("event_seq_rounded", ignorenulls=True).over(w7).alias("last_known_event_seq_rounded"),
    )
)

result = metrics.select("id", "resolved_channel", "first_known_channel_2")
