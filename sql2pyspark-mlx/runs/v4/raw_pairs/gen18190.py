from pyspark.sql import functions as F

lines = (
    df_carts
    .withColumn("line", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

result = (
    lines
    .withColumn("hour_window", F.date_trunc("HOUR", "created_at"))
    .cube("hour_window", "product_id")
    .agg(
        F.round(F.sum("qty") / F.nullif(F.count(F.when(F.col("promo") == "BUNDLE", 1)), F.lit(0)), 2).alias("qty_per_bundle"),
        F.count(F.when(F.col("promo") == "BUNDLE", 1)).alias("bundle_rows"),
        F.count(F.when(F.col("promo") == "WELCOME5", 1)).alias("welcome5_rows"),
        F.grouping("hour_window").alias("grouping_level"),
    )
    .filter(F.col("welcome5_rows") >= 1)
    .orderBy("bundle_rows")
)
