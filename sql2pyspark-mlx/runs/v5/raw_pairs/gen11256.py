from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc())
w2 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc(), "id")

with_windows = (
    df_promotions
    .filter(F.col("promo_code").isin(["BUNDLE", "SPRING10"]))
    .withColumn("discount_pct_cume_dist", F.cume_dist().over(w))
    .withColumn("discount_pct_bucket", F.ntile(5).over(w2))
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("discount_pct_cume_dist") >= 0.75, "PREMIUM").otherwise("STANDARD").alias("discount_pct_cume_dist_tier"),
    )
    .agg(
        F.avg(F.when(F.col("promo_code") == "BUNDLE", F.col("discount_pct_bucket"))).alias("bundle_avg_discount_pct_bucket"),
        F.max("discount_pct_cume_dist").alias("max_discount_pct_cume_dist"),
        F.count(F.when(F.col("promo_code") == "SPRING10", 1)).alias("spring10_count"),
        F.count_if(F.col("channel") == "web").alias("web_events"),
    )
    .filter(F.col("max_discount_pct_cume_dist") >= 0)
    .orderBy("bundle_avg_discount_pct_bucket")
)
