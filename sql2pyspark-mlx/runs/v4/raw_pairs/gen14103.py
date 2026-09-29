from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy("promo_code")
    .agg(
        F.stddev_samp("discount_pct").alias("discount_pct_stddev"),
        F.sum("discount_pct").alias("total_discount_pct"),
        F.avg(F.when(F.col("channel") == "store", F.col("discount_pct"))).alias("store_avg_discount_pct"),
    )
    .filter(F.col("discount_pct_stddev") >= 0)
    .orderBy(F.col("total_discount_pct").asc_nulls_first())
)
