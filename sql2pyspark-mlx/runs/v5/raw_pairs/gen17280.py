from pyspark.sql import functions as F

matched = (
    df_promotions
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & (F.col("e.starts_on") >= F.col("pr.starts_on")) & (F.col("e.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

result = (
    matched
    .groupBy(
        F.date_trunc("MONTH", "starts_on").alias("activity_month"),
        F.when(F.col("promo_id") >= 9, "PREMIUM").otherwise("STANDARD").alias("promo_id_tier"),
        F.when(F.col("promotion_discount_pct") >= 0.3, "HIGH").otherwise("LOW").alias("promotion_discount_pct_tier"),
    )
    .agg(
        F.avg(F.when(F.col("channel") == "mobile", F.col("discount_pct"))).alias("mobile_avg_discount_pct"),
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_count"),
    )
)
