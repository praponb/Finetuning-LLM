from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("discount_pct")
w2 = Window.partitionBy("promo_code").orderBy("ends_on", "id")
w3 = Window.partitionBy("promo_code").orderBy(F.col("row_seq").desc(), "id")

valid = (
    df_promotions
    .filter(F.col("starts_on") < "2023-05-11")
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.coalesce("channel", F.lit("N/A")).alias("channel_filled"),
    )
)

cleaned = (
    valid
    .filter(F.month("ends_on").isin([2, 10, 12]))
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        F.add_months("ends_on", 1).alias("ends_on_plus_months"),
        F.round("discount_pct", 1).alias("discount_pct_rounded"),
    )
)

with_windows = (
    cleaned
    .withColumn("discount_pct_rank", F.rank().over(w))
    .withColumn("row_seq", F.row_number().over(w2))
)

latest = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

pivoted = (
    latest
    .groupBy("channel")
    .pivot("promo_code", ["SPRING10", "BUNDLE"])
    .agg(F.avg("discount_pct").alias("total"), F.count(F.lit(1)).alias("item_count"))
    .select(
        "channel",
        F.col("SPRING10_total").alias("spring10_total"),
        F.col("SPRING10_item_count").alias("spring10_item_count"),
        F.col("BUNDLE_total").alias("bundle_total"),
        F.col("BUNDLE_item_count").alias("bundle_item_count"),
    )
)

result = (
    pivoted
    .select("channel", "spring10_total", "spring10_item_count", "bundle_total")
    .orderBy(F.col("spring10_item_count").desc_nulls_last())
)
