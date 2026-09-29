from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc(), "id")
w2 = Window.partitionBy("promo_code", "channel").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("promo_code").orderBy("discount_pct")
w4 = Window.partitionBy("promo_code", "channel").orderBy(F.col("cumulative_discount_pct").desc_nulls_last(), "id")

cleaned = (
    df_promotions
    .filter((F.col("ends_on") > F.lit("2023-07-14").cast("date")) & (~F.col("channel").eqNullSafe(F.lit("partner"))))
    .select(
        "*",
        F.add_months("starts_on", 12).alias("starts_on_plus_months"),
        F.last_day("ends_on").alias("ends_on_month_end"),
    )
)

deduped = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

metrics = (
    deduped
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        "starts_on_plus_months",
        "ends_on_month_end",
        "latest_rank",
        F.sum("discount_pct").over(w2).alias("cumulative_discount_pct"),
        F.cume_dist().over(w3).alias("discount_pct_cume_dist"),
    )
)

ranked = (
    metrics
    .withColumn("latest_rank_2", F.row_number().over(w4))
    .filter(F.col("latest_rank_2") == 1)
    .select(
        "id",
        "channel",
        "discount_pct",
        "starts_on_plus_months",
        "cumulative_discount_pct",
        "discount_pct_cume_dist",
    )
)

cleaned_promotions = (
    ranked
    .filter(F.month("starts_on_plus_months").isin([3, 4, 8]))
    .select(
        "id",
        "discount_pct",
        "starts_on_plus_months",
        "cumulative_discount_pct",
        F.upper("channel").alias("channel_upper"),
        F.round((F.col("discount_pct") * 0.9), 2).alias("adjusted_discount_pct"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_filled"),
    )
)

totals = (
    cleaned_promotions
    .groupBy(
        F.when(F.col("cumulative_discount_pct") >= 2, "PREMIUM").otherwise("STANDARD").alias("cumulative_discount_pct_tier"),
    )
    .agg(
        F.avg("cumulative_discount_pct").alias("avg_cumulative_discount_pct"),
        F.count("*").alias("num_rows"),
        F.max("cumulative_discount_pct").alias("max_cumulative_discount_pct"),
    )
    .filter(F.col("max_cumulative_discount_pct") > 0)
)

result = (
    totals
    .select(
        "cumulative_discount_pct_tier",
        "avg_cumulative_discount_pct",
        "num_rows",
        "max_cumulative_discount_pct",
    )
    .orderBy(F.col("cumulative_discount_pct_tier").desc_nulls_last())
    .limit(5)
)
