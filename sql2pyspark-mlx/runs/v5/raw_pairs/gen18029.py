from pyspark.sql import functions as F

lines = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "line"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "pos",
        F.col("line.qty"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

result = (
    lines
    .rollup(
        F.when(F.col("qty") >= 5, "PREMIUM").otherwise("STANDARD").alias("qty_tier"),
        F.when(F.col("price") >= 252, "LARGE").otherwise("SMALL").alias("price_tier"),
        "promo",
    )
    .agg(
        F.count(F.when(F.col("promo") == "SPRING10", 1)).alias("spring10_count"),
        F.percentile_approx("qty", [0.25, 0.5, 0.75]).alias("qty_quartiles"),
        F.count(F.when(F.col("promo") == "SPRING10", 1)).alias("spring10_rows"),
    )
    .filter(F.col("spring10_count") < 1000)
    .orderBy(F.col("spring10_count").desc_nulls_last())
)
