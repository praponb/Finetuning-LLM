from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel")
w2 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc())
w3 = Window.partitionBy("promo_code")
w4 = Window.orderBy(F.col("discount_pct").desc())

remaining_keys = (
    df_promotions
    .select("channel")
    .intersect(
        df_promotions
        .filter(F.col("promo_code") == "VIP")
        .select("channel")
    )
)

verified = (
    df_promotions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter(F.col("channel").isNotNull() & (F.col("starts_on") > F.lit("2023-07-28").cast("date")))
    .withColumn("promo_code_lower", F.lower("promo_code"))
)

enriched = (
    filtered
    .filter(F.col("promo_code").isin(["VIP", "BUNDLE"]))
    .select(
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        "promo_code_lower",
        F.max("discount_pct").over(w).alias("channel_max_discount_pct"),
        F.cume_dist().over(w2).alias("discount_pct_cume_dist"),
        F.sum("discount_pct").over(w3).alias("promo_code_sum_discount_pct"),
        F.cume_dist().over(w4).alias("discount_pct_cume_dist_2"),
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w3)), 4).alias("discount_pct_share"),
    )
)

filtered_promotions = (
    enriched
    .filter(F.col("discount_pct") <= 0.28)
    .select(
        "starts_on",
        "ends_on",
        "promo_code_lower",
        "channel_max_discount_pct",
        "discount_pct_cume_dist_2",
        (F.col("promo_code_sum_discount_pct") - F.col("channel_max_discount_pct")).alias("promo_code_sum_discount_pct_minus_channel_max_discount_pct"),
    )
)

result = (
    filtered_promotions
    .select("starts_on", "promo_code_lower", "channel_max_discount_pct")
    .orderBy(F.col("channel_max_discount_pct").desc())
)
