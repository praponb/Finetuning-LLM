from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "promotion_promo_code").orderBy(F.col("is_spring10").desc_nulls_last())

base = (
    df_promotions
    .filter((F.col("starts_on") > F.lit("2023-07-13").cast("date")) & F.col("channel").isNotNull())
    .withColumn("clean_channel", F.trim("channel"))
    .withColumn("is_spring10", F.when(F.col("promo_code") == "SPRING10", 1).otherwise(0))
    .withColumn("channel_clean", F.coalesce("channel", F.lit("DIRECT")))
)

matched = (
    base
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("e.ends_on") >= F.col("pr.starts_on")) & (F.col("e.ends_on") <= F.date_add(F.col("pr.ends_on"), 10)), "inner")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

ranked = (
    matched
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

kept_keys = (
    ranked
    .select("id")
    .subtract(
        df_promotions
        .filter(F.col("channel").isin(["mobile", "partner", "web"]))
        .select("id")
    )
)

reconciled = (
    ranked
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

attributed = (
    reconciled
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & (F.col("e.ends_on") >= F.col("pr.starts_on")) & (F.col("e.ends_on") <= F.date_add(F.col("pr.ends_on"), 7)), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

matched_promotions = (
    attributed
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("s.ends_on") >= F.col("pr.starts_on")) & (F.col("s.ends_on") <= F.date_add(F.col("pr.ends_on"), 7)), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id_3"),
        F.col("pr.promo_code").alias("promotion_promo_code_3"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_3"),
    )
)

result = (
    matched_promotions
    .groupBy("channel", "promotion_promo_code_3")
    .agg(
        F.count(F.when(F.col("promotion_promo_code") == "VIP", 1)).alias("vip_rows"),
        F.sum(F.when(F.col("promotion_promo_code_2") == "WELCOME5", F.col("promo_id_3")).otherwise(0)).alias("welcome5_promo_id_3"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("welcome5_promo_id_3") >= 0)
    .orderBy(F.col("vip_rows").desc())
)
