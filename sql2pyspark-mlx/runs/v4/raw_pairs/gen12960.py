from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc(), "id")
w2 = Window.orderBy("discount_pct_whole", "id", "promo_id")
w3 = Window.partitionBy("channel").orderBy(F.col("discount_pct_whole").desc(), "id", "promo_id")
w4 = Window.partitionBy("channel").orderBy("latest_rank", "id", "promo_id")

filtered = (
    df_promotions
    .filter(F.col("starts_on") >= F.lit("2023-05-20").cast("date"))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.substring("channel", 1, 3).alias("channel_prefix"),
        F.ceil("discount_pct").alias("discount_pct_whole"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_filled"),
    )
)

latest = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

filtered_promotions = (
    latest
    .filter((F.col("starts_on") > F.lit("2023-06-28").cast("date")) & F.col("channel").isNotNull() & (F.col("discount_pct") <= 0.22))
    .select(
        "*",
        F.date_add("ends_on", 4).alias("ends_on_plus_4d"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_filled_2"),
    )
)

attributed = (
    filtered_promotions
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & (F.col("e.ends_on") >= F.col("pr.starts_on")) & (F.col("e.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "inner")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

with_windows = (
    attributed
    .select(
        "*",
        F.ntile(10).over(w2).alias("discount_pct_whole_bucket"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

deduped = (
    with_windows
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

matched_keys = (
    deduped
    .select("channel", "id")
    .subtract(
        df_promotions
        .filter(~F.col("promo_code").isin(["WELCOME5", "BUNDLE", "SPRING10"]))
        .select("channel", "id")
    )
)

verified = (
    deduped
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "channel",
        "starts_on",
        "ends_on",
        "discount_pct_whole",
        "channel_filled",
        "ends_on_plus_4d",
        "channel_filled_2",
        "promo_id",
        "promotion_promo_code",
    )
    .orderBy(F.col("promotion_promo_code").desc_nulls_last())
)
