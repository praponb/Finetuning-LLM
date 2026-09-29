from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy(
        F.when(F.col("discount_pct") >= 0.31, "HIGH").otherwise("LOW").alias("discount_pct_tier"),
    )
    .agg(
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.percentile_approx("discount_pct", 0.9).alias("discount_pct_p90"),
        F.count(F.when(F.col("promo_code") == "SPRING10", 1)).alias("spring10_count"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
    )
    .filter(F.col("num_channels") > 1)
)
