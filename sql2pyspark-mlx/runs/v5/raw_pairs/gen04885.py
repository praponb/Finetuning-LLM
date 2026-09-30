from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("starts_on").desc())

cleaned = (
    df_promotions
    .filter(F.col("discount_pct") < 0.24)
    .select(
        "*",
        F.ceil("discount_pct").alias("discount_pct_whole"),
        F.substring("promo_code", 1, 4).alias("promo_code_prefix"),
        F.coalesce("channel", F.lit("DIRECT")).alias("resolved_channel"),
    )
)

valid = (
    cleaned
    .filter((F.col("starts_on") >= F.lit("2023-08-21").cast("date")) & (F.year("ends_on") >= 2023))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        "discount_pct_whole",
        "promo_code_prefix",
        "resolved_channel",
        F.upper("channel").alias("channel_upper"),
    )
)

with_window = (
    valid
    .alias("a")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("a.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("a.ends_on") >= F.col("pr.starts_on")) & (F.col("a.ends_on") <= F.date_add(F.col("pr.ends_on"), 7)), "inner")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

latest = (
    with_window
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "starts_on",
        "ends_on",
        "discount_pct_whole",
        "promo_code_prefix",
        "channel_upper",
        "promo_id",
        "promotion_promo_code",
    )
)

result = (
    latest
    .groupBy(
        F.when(F.col("promo_id") >= 9, "HIGH").otherwise("LOW").alias("promo_id_tier"),
        F.date_trunc("WEEK", "ends_on").alias("week_start"),
    )
    .agg(
        F.sort_array(F.collect_set("promotion_promo_code")).alias("promotion_promo_code_list"),
        F.avg("discount_pct_whole").alias("avg_discount_pct_whole"),
    )
    .orderBy("week_start")
)
