from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy(
        F.when(F.col("discount_pct") >= 0.27, "PREMIUM").otherwise("STANDARD").alias("discount_pct_tier"),
    )
    .agg(
        F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct"),
        F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct_2"),
        F.sum(F.when(F.col("channel") == "web", F.col("discount_pct")).otherwise(0)).alias("web_discount_pct"),
    )
    .filter(F.col("avg_discount_pct_2") > 0)
)
