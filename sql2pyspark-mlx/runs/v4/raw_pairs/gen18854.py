from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "promotion_promo_code")
w2 = Window.partitionBy("promotion_promo_code", "channel").orderBy(F.col("promo_id").desc_nulls_first())

filtered = (
    df_promotions
    .filter((F.col("ends_on") > F.lit("2024-05-06").cast("date")) & F.col("channel").isNotNull() & F.col("starts_on").between("2023-05-01", "2023-08-29"))
    .select(
        "*",
        F.datediff("ends_on", "starts_on").alias("days_starts_on_to_ends_on"),
        F.ceil("discount_pct").alias("discount_pct_whole"),
    )
)

attributed = (
    filtered
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel").eqNullSafe(F.col("pr.channel"))) & F.col("a.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

matched_keys = (
    attributed
    .select("channel")
    .intersect(
        df_promotions
        .filter(F.col("discount_pct").between(0.31, 0.32))
        .select("channel")
    )
)

verified = (
    attributed
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

metrics = (
    verified
    .select(
        "*",
        F.round((F.col("promotion_discount_pct") / F.sum("promotion_discount_pct").over(w)), 4).alias("promotion_discount_pct_share"),
        F.percent_rank().over(w2).alias("promo_id_percent_rank"),
    )
)

valid = (
    metrics
    .filter(F.col("starts_on") > F.lit("2023-06-01").cast("date"))
    .withColumn("greatest_promotion_discount_pct_share_discount_pct_whole", F.greatest("promotion_discount_pct_share", "discount_pct_whole"))
    .withColumn("promotion_discount_pct_share_int", F.col("promotion_discount_pct_share").cast("int"))
)

matched = (
    valid
    .alias("src")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("src.channel"), F.lit("store")) == F.col("pr.channel")) & (F.col("src.starts_on") >= F.col("pr.starts_on")) & (F.col("src.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "inner")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

base = (
    matched
    .filter((F.col("ends_on") >= F.lit("2024-06-11").cast("date")) & F.col("promotion_promo_code_2").isNotNull())
    .withColumn("is_welcome5", F.when(F.col("promotion_promo_code") == "WELCOME5", 1).otherwise(0))
    .withColumn("promotion_discount_pct_share_int_rounded", F.round("promotion_discount_pct_share_int", 0))
)

result = (
    base
    .groupBy("channel", "promotion_promo_code_2")
    .pivot("promo_code", ["SPRING10", "WELCOME5"])
    .agg(F.avg("promo_id").alias("amount"))
)
