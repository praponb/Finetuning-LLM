from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy(
        F.when(F.col("discount_pct") >= 0.29, "LARGE").otherwise("SMALL").alias("discount_pct_tier"),
    )
    .agg(
        F.percentile_approx("discount_pct", [0.25, 0.5, 0.75]).alias("discount_pct_quartiles"),
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_count"),
    )
    .orderBy(F.col("web_count").asc_nulls_first())
)
