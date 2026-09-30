from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("starts_on", "id")
w2 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc())
w3 = Window.partitionBy("promo_code").orderBy("starts_on_minus_54d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc())
w5 = Window.partitionBy("channel").orderBy("starts_on", "id")

valid = (
    df_promotions
    .filter((F.col("starts_on") > F.lit("2023-08-07").cast("date")) & F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "discount_pct",
        F.date_sub("starts_on", 54).alias("starts_on_minus_54d"),
        F.concat_ws("/", "channel", "promo_code").alias("channel_promo_code_key"),
    )
)

deduped = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

metrics = (
    deduped
    .select(
        "*",
        F.cume_dist().over(w2).alias("discount_pct_cume_dist"),
        F.last("channel", ignorenulls=True).over(w3).alias("last_known_channel"),
        F.cume_dist().over(w4).alias("discount_pct_cume_dist_2"),
    )
)

base = (
    metrics
    .filter(F.col("channel").isNotNull() & (F.col("discount_pct") >= 0.15))
    .select(
        "*",
        F.concat("promo_code", F.lit(", "), "channel").alias("promo_code_channel"),
    )
)

latest = (
    base
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "discount_pct",
        "channel_promo_code_key",
        "discount_pct_cume_dist",
        "last_known_channel",
        "discount_pct_cume_dist_2",
        "promo_code_channel",
    )
)

result = (
    latest
    .select(
        "id",
        "discount_pct",
        "channel_promo_code_key",
        "discount_pct_cume_dist",
        "last_known_channel",
        "promo_code_channel",
    )
    .orderBy(F.col("id").asc_nulls_first())
)
