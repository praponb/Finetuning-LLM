from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy(
        "channel",
        F.when(F.col("discount_pct") >= 0.32, "HIGH").otherwise("LOW").alias("discount_pct_tier"),
    )
    .agg(
        F.avg("discount_pct").alias("avg_discount_pct"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.stddev_samp("discount_pct").alias("discount_pct_stddev"),
    )
    .filter(F.col("discount_pct_stddev") > 0)
    .orderBy("discount_pct_tier")
)
