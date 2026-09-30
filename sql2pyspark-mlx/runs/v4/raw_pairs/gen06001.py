from pyspark.sql import functions as F

result = (
    df_promotions
    .cube(F.date_trunc("YEAR", "ends_on").alias("year_start"))
    .agg(
        F.round(F.sum("discount_pct") / F.nullif(F.count(F.when(F.col("promo_code") == "WELCOME5", 1)), F.lit(0)), 2).alias("discount_pct_per_welcome5"),
        F.stddev_samp("discount_pct").alias("discount_pct_stddev"),
    )
    .orderBy(F.col("discount_pct_per_welcome5").desc())
)
