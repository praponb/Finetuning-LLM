from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy(F.date_trunc("QUARTER", "ends_on").alias("quarter_start"), "channel")
    .agg(
        F.percentile_approx("discount_pct", 0.95).alias("discount_pct_p95"),
        F.sum("discount_pct").alias("total_discount_pct"),
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_rows"),
        F.sum(F.when(F.col("promo_code") == "VIP", F.col("discount_pct"))).alias("vip_total_discount_pct"),
        F.sum(F.when(F.col("channel") == "partner", F.col("discount_pct")).otherwise(0)).alias("partner_discount_pct"),
    )
)
