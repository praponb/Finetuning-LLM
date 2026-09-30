from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("days_since_ends_on", "id")
w_ordered = Window.partitionBy("channel").orderBy("days_since_ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.orderBy("running_min_days_since_ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part_2 = Window.partitionBy("channel").orderBy("running_count_days_since_ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("channel").orderBy("days_since_ends_on_adjusted", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("channel").orderBy("last_known_days_since_ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("last_known_days_since_ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("channel").orderBy("running_min_days_since_ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("channel").orderBy("days_since_ends_on_adjusted", "id")
w10 = Window.partitionBy("channel").orderBy("running_max_days_since_ends_on", "id")
w11 = Window.partitionBy("channel").orderBy(F.col("first_known_last_known_days_since_ends_on").desc_nulls_first())

base = (
    df_promotions
    .filter((F.col("starts_on") >= F.lit("2023-07-13").cast("date")) & (~F.col("channel").eqNullSafe(F.lit("web"))))
    .select(
        "id",
        "channel",
        "ends_on",
        "discount_pct",
        F.datediff(F.current_date(), "ends_on").alias("days_since_ends_on"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_filled"),
    )
)

ranked = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

enriched = (
    ranked
    .select(
        "*",
        F.max("days_since_ends_on").over(w_ordered).alias("running_max_days_since_ends_on"),
        F.count("days_since_ends_on").over(w_ordered).alias("running_count_days_since_ends_on"),
        F.avg("days_since_ends_on").over(w_ordered).alias("running_avg_days_since_ends_on"),
        F.min("days_since_ends_on").over(w_ordered).alias("running_min_days_since_ends_on"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "channel",
        "days_since_ends_on",
        "channel_filled",
        "running_max_days_since_ends_on",
        "running_count_days_since_ends_on",
        "running_min_days_since_ends_on",
        F.last("days_since_ends_on", ignorenulls=True).over(w_part).alias("last_known_days_since_ends_on"),
        F.last("running_avg_days_since_ends_on", ignorenulls=True).over(w_part_2).alias("last_known_running_avg_days_since_ends_on"),
    )
)

base_promotions = (
    scored
    .filter(F.col("channel").isNotNull() & (F.col("channel").eqNullSafe(F.lit("partner"))))
    .withColumn("days_since_ends_on_adjusted", F.round((F.col("days_since_ends_on") * 1.07), 2))
    .withColumn("channel_clean", F.coalesce("channel", F.lit("NONE")))
)

with_windows = (
    base_promotions
    .filter(F.col("days_since_ends_on_adjusted") > 65)
    .select(
        "*",
        F.sum("running_max_days_since_ends_on").over(w5).alias("running_running_max_days_since_ends_on"),
        F.sum("last_known_running_avg_days_since_ends_on").over(w6).alias("cumulative_last_known_running_avg_days_since_ends_on"),
        F.first("last_known_days_since_ends_on", ignorenulls=True).over(w7).alias("first_known_last_known_days_since_ends_on"),
    )
)

result = (
    with_windows
    .select(
        "*",
        F.sum("last_known_running_avg_days_since_ends_on").over(w8).alias("cumulative_last_known_running_avg_days_since_ends_on_2"),
        F.ntile(10).over(w_running).alias("days_since_ends_on_adjusted_bucket"),
        F.lead("running_running_max_days_since_ends_on", 1).over(w10).alias("next_running_running_max_days_since_ends_on"),
        F.rank().over(w11).alias("first_known_last_known_days_since_ends_on_rank"),
    )
)
