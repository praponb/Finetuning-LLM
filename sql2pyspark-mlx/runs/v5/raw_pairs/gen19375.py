from pyspark.sql import functions as F

matched = (
    df_orders
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("src.order_date") >= F.col("pr.starts_on")) & (F.col("src.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

result = (
    matched
    .cube(F.date_trunc("year", "order_date").alias("year_start"), "store_id")
    .agg(
        F.round(F.sum("discount_pct") / F.nullif(F.count(F.when(F.col("channel") == "partner", 1)), F.lit(0)), 2).alias("discount_pct_per_partner"),
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_count"),
        F.min("discount").alias("min_discount"),
        F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("cancelled_rows"),
        F.sum(F.when(F.col("status") == "SHIPPED", F.col("shipping_fee")).otherwise(0)).alias("shipped_shipping_fee"),
        F.stddev_samp("amount").alias("amount_stddev"),
    )
    .filter(F.col("cancelled_rows") > 1)
)
