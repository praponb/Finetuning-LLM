from pyspark.sql import functions as F

result = (
    df_promotions
    .cube(
        F.when(F.col("discount_pct") >= 0.3, "HIGH").otherwise("LOW").alias("discount_pct_tier"),
    )
    .agg(
        F.percentile_approx("discount_pct", 0.5).alias("discount_pct_p50"),
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_count"),
        F.max(F.when(F.col("channel") == "store", F.col("discount_pct"))).alias("store_max_discount_pct"),
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_count"),
        F.max(F.when(F.col("channel") == "web", F.col("discount_pct"))).alias("web_max_discount_pct"),
    )
)
