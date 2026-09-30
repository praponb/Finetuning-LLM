from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy(
        F.when(F.col("discount_pct") >= 0.32, "PREMIUM").otherwise("STANDARD").alias("discount_pct_tier"),
    )
    .agg(
        F.min("discount_pct").alias("min_discount_pct"),
        F.stddev_samp("discount_pct").alias("discount_pct_stddev"),
    )
    .orderBy(F.col("discount_pct_stddev").desc())
)
