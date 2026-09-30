from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("ends_on", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("promotion_promo_code").orderBy("promotion_discount_pct", "id", "promo_id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("promo_code").orderBy("starts_on", "id", "promo_id").rowsBetween(-6, -1)

base = (
    df_promotions
    .filter(F.col("ends_on") > F.lit("2023-08-25").cast("date"))
    .withColumn("ends_on_minus_22d", F.date_sub("ends_on", 22))
)

attributed = (
    base
    .alias("e")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("e.channel"), F.lit("web")) == F.col("pr.channel")) & F.col("e.ends_on_minus_22d").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

metrics = (
    attributed
    .filter(F.col("promo_id") != 6)
    .select(
        "*",
        F.sum("promotion_discount_pct").over(w).alias("running_promotion_discount_pct"),
        F.avg("discount_pct").over(w2).alias("discount_pct_avg_5"),
        F.avg("promotion_discount_pct").over(w3).alias("promotion_discount_pct_avg_6_prior"),
    )
)

remaining_keys = (
    metrics
    .select("channel", "id")
    .intersectAll(
        df_promotions
        .filter(F.col("starts_on") <= "2023-12-01")
        .select("channel", "id")
    )
)

verified = (
    metrics
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.when(F.col("promotion_discount_pct_avg_6_prior") >= 0, "PREMIUM").otherwise("STANDARD").alias("promotion_discount_pct_avg_6_prior_tier"),
        F.when(F.col("promotion_discount_pct") >= 0.35, "PREMIUM").otherwise("STANDARD").alias("promotion_discount_pct_tier"),
        F.when(F.col("running_promotion_discount_pct") >= 2, "PREMIUM").otherwise("STANDARD").alias("running_promotion_discount_pct_tier"),
    )
    .agg(
        F.count_if(F.col("channel") == "store").alias("store_events"),
        F.count(F.when(F.col("promotion_promo_code") == "WELCOME5", 1)).alias("welcome5_count"),
    )
    .filter(F.col("store_events") < 1000)
    .orderBy(F.col("promotion_discount_pct_avg_6_prior_tier").desc_nulls_last())
)
