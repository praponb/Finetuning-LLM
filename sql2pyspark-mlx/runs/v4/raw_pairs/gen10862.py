from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy("is_bundle", "id")
w_hist = Window.partitionBy("promo_code").orderBy("row_num", "id", "promo_id").rowsBetween(-6, Window.currentRow)
w_recent = Window.partitionBy("channel").orderBy("discount_pct", "id", "promo_id")

base = (
    df_promotions
    .filter((F.col("starts_on") > F.lit("2023-07-09").cast("date")) & F.col("channel").isNotNull())
    .select(
        "*",
        F.when(F.col("promo_code") == "BUNDLE", 1).otherwise(0).alias("is_bundle"),
    )
)

deduped = (
    base
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

attributed = (
    deduped
    .alias("src")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("src.channel"), F.lit("store")) == F.col("pr.channel")) & (F.col("src.ends_on") >= F.col("pr.starts_on")) & (F.col("src.ends_on") <= F.date_add(F.col("pr.ends_on"), 3)), "inner")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

cleaned = (
    attributed
    .filter(F.col("promotion_promo_code").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        "is_bundle",
        "row_num",
        "promo_id",
        "promotion_promo_code",
        F.trim("promotion_promo_code").alias("clean_promotion_promo_code"),
        F.coalesce("channel", F.lit("OTHER")).alias("resolved_channel"),
    )
)

result = (
    cleaned
    .withColumn("promo_id_avg_6", F.avg("promo_id").over(w_hist))
    .withColumn("next_discount_pct", F.lead("discount_pct", 2).over(w_recent))
)
