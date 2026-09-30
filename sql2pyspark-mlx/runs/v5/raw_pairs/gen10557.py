from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("avg_discount_pct").desc())

result = (
    df_promotions
    .groupBy("channel", F.date_trunc("MONTH", "ends_on").alias("activity_month"))
    .agg(
        F.avg("discount_pct").alias("avg_discount_pct"),
        F.sum(F.when(F.col("promo_code") == "VIP", F.col("discount_pct")).otherwise(0)).alias("vip_discount_pct"),
        F.percentile_approx("discount_pct", 0.9).alias("discount_pct_p90"),
    )
    .withColumn("avg_discount_pct_rank", F.rank().over(w))
    .orderBy(F.col("channel").desc())
)
