from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc())

latest = (
    df_promotions
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .groupBy(
        F.date_trunc("MONTH", "ends_on").alias("activity_month"),
        F.when(F.col("discount_pct") >= 0.34, "LARGE").otherwise("SMALL").alias("discount_pct_tier"),
    )
    .agg(
        F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct"),
        F.countDistinct("channel").alias("distinct_channels"),
        F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct_2"),
        F.percentile_approx("discount_pct", 0.95).alias("discount_pct_p95"),
    )
    .filter(F.col("avg_discount_pct_2") < 0)
    .orderBy(F.col("avg_discount_pct").desc())
)
