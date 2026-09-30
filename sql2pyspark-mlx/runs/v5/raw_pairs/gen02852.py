from pyspark.sql import functions as F

lines = (
    df_carts
    .withColumn("item", F.explode_outer("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("item.product_id"),
        F.col("item.price"),
        F.col("item.promo"),
    )
)

result = (
    lines
    .withColumn("hour_window", F.date_trunc("HOUR", "created_at"))
    .groupingSets([["hour_window", "promo"], ["hour_window"]], "hour_window", "promo")
    .agg(
        F.sum(F.when(F.col("promo") == "WELCOME5", F.col("product_id")).otherwise(0)).alias("welcome5_product_id"),
        F.percentile_approx("product_id", [0.25, 0.5, 0.75]).alias("product_id_quartiles"),
    )
    .orderBy(F.col("welcome5_product_id").desc_nulls_last())
)
