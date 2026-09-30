from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("ends_on", "id")

ranked = (
    df_promotions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

filtered = (
    ranked
    .filter(F.col("ends_on") > F.to_date(F.lit("2023-12-08")))
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        "rn",
        F.concat("promo_code", F.lit(" - "), "channel").alias("promo_code_channel"),
        F.when(F.col("promo_code") == "BUNDLE", 1).otherwise(0).alias("is_bundle"),
    )
)

matched = (
    filtered
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & (F.col("s.ends_on") >= F.col("pr.starts_on")) & (F.col("s.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

matched_keys = (
    matched
    .select("channel", "id")
    .subtract(
        df_promotions
        .filter(F.col("promo_code") == "BUNDLE")
        .select("channel", "id")
    )
)

reconciled = (
    matched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("promo_code")
    .pivot("promotion_promo_code", ["BUNDLE", "VIP"])
    .agg(
        F.max("discount_pct").alias("value_sum"),
        F.count(F.lit(1)).alias("item_count"),
    )
    .select(
        "promo_code",
        F.col("BUNDLE_value_sum").alias("bundle_value_sum"),
        F.col("BUNDLE_item_count").alias("bundle_item_count"),
        F.col("VIP_value_sum").alias("vip_value_sum"),
        F.col("VIP_item_count").alias("vip_item_count"),
    )
)
