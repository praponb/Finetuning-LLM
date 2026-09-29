from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("starts_on").desc(), "id")
w_ordered = Window.partitionBy("promo_code").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("promo_code").orderBy("running_max_days_since_starts_on", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("promo_code").orderBy("starts_on", "id").rowsBetween(-2, Window.currentRow)
w_hist = Window.partitionBy("promo_code").orderBy("running_count_days_since_starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("cumulative_days_since_starts_on", "id")

filtered = (
    df_promotions
    .filter((F.col("channel").eqNullSafe(F.lit("store"))) & F.col("channel").isNotNull())
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        F.datediff(F.current_date(), "starts_on").alias("days_since_starts_on"),
        F.trim("promo_code").alias("clean_promo_code"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_clean"),
    )
)

latest = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

scored = (
    latest
    .filter(F.col("promo_code") == "VIP")
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        "days_since_starts_on",
        "channel_clean",
        "latest_rank",
        F.count("days_since_starts_on").over(w_ordered).alias("running_count_days_since_starts_on"),
        F.avg("days_since_starts_on").over(w_ordered).alias("running_avg_days_since_starts_on"),
        F.max("days_since_starts_on").over(w_ordered).alias("running_max_days_since_starts_on"),
    )
)

scored_promotions = (
    scored
    .select(
        "id",
        "starts_on",
        "ends_on",
        "days_since_starts_on",
        "channel_clean",
        "latest_rank",
        "running_avg_days_since_starts_on",
        "running_max_days_since_starts_on",
        F.avg("days_since_starts_on").over(w3).alias("days_since_starts_on_avg_5"),
        F.avg("running_count_days_since_starts_on").over(w4).alias("running_count_days_since_starts_on_avg_2"),
        F.sum("days_since_starts_on").over(w_hist).alias("cumulative_days_since_starts_on"),
    )
)

scored_promotions_2 = (
    scored_promotions
    .select(
        "id",
        "starts_on",
        "latest_rank",
        "running_max_days_since_starts_on",
        F.lag("running_avg_days_since_starts_on").over(w6).alias("prev_running_avg_days_since_starts_on"),
        F.lead("running_avg_days_since_starts_on").over(w6).alias("next_running_avg_days_since_starts_on"),
        F.lag("running_count_days_since_starts_on_avg_2").over(w6).alias("prev_running_count_days_since_starts_on_avg_2"),
        F.lead("running_count_days_since_starts_on_avg_2").over(w6).alias("next_running_count_days_since_starts_on_avg_2"),
        F.row_number().over(w6).alias("row_seq"),
    )
)

result = (
    scored_promotions_2
    .groupBy(F.date_trunc("week", "starts_on").alias("week_start"))
    .agg(
        F.min("prev_running_count_days_since_starts_on_avg_2").alias("min_prev_running_count_days_since_starts_on_avg_2"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy("min_prev_running_count_days_since_starts_on_avg_2")
)
