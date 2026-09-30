from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("starts_on").desc(), "id")

filtered = (
    df_promotions
    .filter(F.col("channel").isNotNull() & (F.col("discount_pct") >= 0.24))
    .withColumn("discount_pct_int", F.col("discount_pct").cast("int"))
    .withColumn("channel_clean", F.coalesce("channel", F.lit("UNKNOWN")))
)

deduped = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

attributed = (
    deduped
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("a.starts_on") >= F.col("pr.starts_on")) & (F.col("a.starts_on") <= F.date_add(F.col("pr.ends_on"), 3)), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

matched = (
    attributed
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel") == F.col("pr.channel")) & F.col("src.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

valid = (
    matched
    .filter(F.col("promo_id") != 8)
    .select(
        "*",
        F.coalesce("promotion_discount_pct", F.lit(0)).alias("promotion_discount_pct_filled"),
        F.coalesce("promotion_promo_code_2", F.lit("UNKNOWN")).alias("resolved_promotion_promo_code_2"),
    )
)

result = (
    valid
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "discount_pct_int",
        "channel_clean",
        "promo_id",
        "promotion_promo_code",
        "promo_id_2",
    )
    .orderBy(F.col("promotion_promo_code").desc_nulls_last())
)
