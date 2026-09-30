from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("discount_pct")
w3 = Window.partitionBy("channel", "promo_code").orderBy(F.col("discount_pct_percent_rank").desc_nulls_first(), "id")

scored = (
    df_promotions
    .select(
        "*",
        F.first("channel", ignorenulls=True).over(w_ordered).alias("first_known_channel"),
        F.percent_rank().over(w2).alias("discount_pct_percent_rank"),
    )
)

ranked = (
    scored
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

kept_keys = (
    ranked
    .select("id", "promo_code")
    .subtract(
        df_promotions
        .filter(~F.col("promo_code").isin(["WELCOME5", "BUNDLE", "VIP"]))
        .select("id", "promo_code")
    )
)

reconciled = (
    ranked
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

matched = (
    reconciled
    .alias("s")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("s.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("s.ends_on") >= F.col("pr.starts_on")) & (F.col("s.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

result = (
    matched
    .groupBy(
        F.when(F.col("promo_id") >= 11, "HIGH").otherwise("LOW").alias("promo_id_tier"),
        F.when(F.col("discount_pct") >= 0.28, "PREMIUM").otherwise("STANDARD").alias("discount_pct_tier"),
    )
    .agg(
        F.max("discount_pct").alias("max_discount_pct"),
        F.count(F.when(F.col("promotion_promo_code") == "BUNDLE", 1)).alias("bundle_count"),
        F.percentile_approx("promotion_discount_pct", 0.5).alias("promotion_discount_pct_p50"),
    )
    .orderBy("promo_id_tier")
)
