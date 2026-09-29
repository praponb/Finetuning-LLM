from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy(F.col("discount_pct").desc(), "id")
w2 = Window.orderBy("discount_pct_scaled", "id")
w3 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct_scaled").desc_nulls_first())
w4 = Window.orderBy("ends_on", "id").rowsBetween(-2, Window.currentRow)
w_grp = Window.partitionBy("promo_code").orderBy("starts_on", "id")
w6 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct_scaled_bucket").desc(), "id")
w7 = Window.partitionBy("promo_code").orderBy("discount_pct_scaled")

filtered = (
    df_promotions
    .filter(F.col("channel").isNotNull())
    .select(
        "*",
        F.last_day("ends_on").alias("ends_on_month_end"),
        F.round((F.col("discount_pct") * 100), 2).alias("discount_pct_scaled"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_clean"),
    )
)

ranked = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

valid = (
    ranked
    .filter(F.col("channel").isNotNull() & (F.col("starts_on") >= F.lit("2023-07-12").cast("date")))
    .select(
        "*",
        F.length("channel").alias("channel_length"),
        F.trim("channel").alias("clean_channel"),
    )
)

scored = (
    valid
    .select(
        "id",
        "promo_code",
        "starts_on",
        "discount_pct_scaled",
        "channel_clean",
        "clean_channel",
        F.ntile(10).over(w2).alias("discount_pct_scaled_bucket"),
        F.rank().over(w3).alias("discount_pct_scaled_rank"),
        F.avg("channel_length").over(w4).alias("channel_length_avg_2"),
    )
)

enriched = (
    scored
    .select(
        "*",
        F.row_number().over(w_grp).alias("event_seq"),
        F.ntile(5).over(w6).alias("discount_pct_scaled_bucket_bucket"),
        F.dense_rank().over(w7).alias("discount_pct_scaled_dense_rank"),
    )
)

result = (
    enriched
    .select(
        "id",
        "discount_pct_scaled",
        "event_seq",
        "discount_pct_scaled_bucket_bucket",
    )
    .orderBy(F.col("discount_pct_scaled").asc_nulls_last(), F.col("event_seq").desc())
)
