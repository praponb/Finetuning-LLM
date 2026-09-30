from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("promo_code", "channel").orderBy("ends_on_minus_44d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("channel", "promo_code").orderBy("starts_on", "id")
w3 = Window.partitionBy("promo_code").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("channel").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("channel", "promo_code").orderBy(F.col("starts_on").desc(), "id")
w6 = Window.partitionBy("channel").orderBy(F.col("row_seq").desc(), "id")
w7 = Window.partitionBy("promo_code").orderBy(F.col("row_seq").desc(), "id")
w8 = Window.partitionBy("channel").orderBy("is_bundle", "id")
w9 = Window.partitionBy("channel").orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_promotions
    .filter((F.col("ends_on") > F.lit("2023-08-15").cast("date")) & (~F.col("channel").eqNullSafe(F.lit("web"))))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        F.date_sub("ends_on", 44).alias("ends_on_minus_44d"),
    )
)

with_windows = (
    filtered
    .withColumn("first_known_channel", F.first("channel", ignorenulls=True).over(w_part))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("last_known_channel", F.last("channel", ignorenulls=True).over(w3))
    .withColumn("first_known_channel_2", F.first("channel", ignorenulls=True).over(w_grp))
)

cleaned = (
    with_windows
    .filter((F.col("ends_on") >= F.lit("2023-11-08").cast("date")) & (F.col("ends_on") >= "2025-09-29"))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "ends_on_minus_44d",
        "row_seq",
        "last_known_channel",
        "first_known_channel_2",
        F.when(F.col("row_seq") >= 8, "High").when(F.col("row_seq") >= 3, "Medium").otherwise("Low").alias("row_seq_band"),
        F.when(F.col("promo_code") == "BUNDLE", 1).otherwise(0).alias("is_bundle"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_filled"),
    )
)

ranked = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "channel",
        "promo_code",
        "row_seq",
        "last_known_channel",
        "row_seq_band",
        "is_bundle",
    )
)

latest = (
    ranked
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
)

ranked_promotions = (
    latest
    .withColumn("rank_in_group", F.row_number().over(w7))
    .filter(F.col("rank_in_group") == 1)
)

metrics = (
    ranked_promotions
    .filter(F.col("promo_code").isin(["SPRING10", "WELCOME5", "VIP"]))
    .select(
        "*",
        F.ntile(3).over(w8).alias("is_bundle_bucket"),
        F.first("is_bundle", ignorenulls=True).over(w9).alias("first_known_is_bundle"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("row_seq") >= 7, "LARGE").otherwise("SMALL").alias("row_seq_tier"),
        "channel",
    )
    .agg(
        F.count(F.when(F.col("promo_code") == "SPRING10", 1)).alias("spring10_rows"),
        F.avg(F.when(F.col("promo_code") == "VIP", F.col("is_bundle_bucket"))).alias("vip_avg_is_bundle_bucket"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("spring10_rows").desc())
)
