from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "promo_code")
w2 = Window.partitionBy("channel", "promo_code").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("promotion_promo_code", "channel").orderBy(F.col("promo_id").asc_nulls_last())
w5 = Window.partitionBy("promotion_promo_code_2")

enriched = (
    df_promotions
    .select(
        "*",
        F.max("discount_pct").over(w).alias("channel_max_discount_pct"),
        F.first("channel", ignorenulls=True).over(w2).alias("first_known_channel"),
        F.first("channel", ignorenulls=True).over(w3).alias("first_known_channel_2"),
    )
)

matched = (
    enriched
    .alias("s")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("s.channel"), F.lit("web")) == F.col("pr.channel")) & F.col("s.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

kept_keys = (
    matched
    .select("channel", "promo_code")
    .intersect(
        df_promotions
        .filter(F.col("promo_code") != "WELCOME5")
        .select("channel", "promo_code")
    )
)

verified = (
    matched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")) & (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

matched_promotions = (
    verified
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel") == F.col("pr.channel")) & F.col("a.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

base = (
    matched_promotions
    .filter((F.col("starts_on") > F.lit("2023-09-22").cast("date")) & (F.col("promo_code") == "SPRING10"))
    .withColumn("clean_promo_code", F.trim("promo_code"))
)

enriched_promotions = (
    base
    .select(
        "*",
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w)), 4).alias("discount_pct_share"),
        F.dense_rank().over(w4).alias("promo_id_dense_rank"),
        F.round((F.col("channel_max_discount_pct") / F.sum("channel_max_discount_pct").over(w5)), 4).alias("channel_max_discount_pct_share"),
    )
)

kept_keys_2 = (
    enriched_promotions
    .select("promo_code")
    .intersectAll(
        df_promotions
        .filter(F.col("ends_on").between("2023-10-01", "2024-01-01"))
        .select("promo_code")
    )
)

kept_rows = (
    enriched_promotions
    .alias("s")
    .join(kept_keys_2.alias("k"), (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        "channel_max_discount_pct",
        "first_known_channel",
        "first_known_channel_2",
        F.when(F.col("promo_code").rlike("^(SPRING10|VIP)$"), "OTHER_ZONE").when(F.col("promo_code").rlike("^(BUNDLE|WELCOME5)$"), "CORE").otherwise("GROUP_A").alias("promo_code_zone"),
    )
    .orderBy(F.col("promo_code_zone").desc())
)
