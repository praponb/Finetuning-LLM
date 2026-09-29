from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("channel").orderBy("ends_on", "id")
w_recent = Window.partitionBy("channel").orderBy(F.col("ends_on").desc(), "id")
w_hist = Window.partitionBy("channel")
w5 = Window.orderBy("starts_on", "id")
w6 = Window.partitionBy("channel").orderBy(F.col("event_seq_2").desc(), "id")

cleaned = (
    df_promotions
    .filter(F.col("channel").isin(["mobile", "store", "web"]))
    .withColumn("discount_pct_level", F.when(F.col("discount_pct") >= 0.33, "High").when(F.col("discount_pct") >= 0.19, "Medium").otherwise("Low"))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("N/A")))
)

cleaned_promotions = (
    cleaned
    .filter((F.col("starts_on") >= F.lit("2023-09-06").cast("date")) & F.col("channel").isNotNull())
    .select(
        "*",
        F.substring("promo_code", 1, 4).alias("promo_code_prefix"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_filled"),
    )
)

with_windows = (
    cleaned_promotions
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "discount_pct_level",
        F.first("channel", ignorenulls=True).over(w).alias("first_known_channel"),
        F.row_number().over(w2).alias("event_seq"),
        F.row_number().over(w_recent).alias("event_seq_2"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "channel",
        "first_known_channel",
        "event_seq_2",
        F.avg("event_seq").over(w_hist).alias("channel_avg_event_seq"),
        F.lag("event_seq_2", 1).over(w5).alias("prev_event_seq_2"),
        F.ntile(5).over(w6).alias("event_seq_2_bucket"),
    )
)

result = (
    metrics
    .select("id", "prev_event_seq_2", "event_seq_2_bucket")
    .orderBy(F.col("prev_event_seq_2").desc_nulls_first())
)
