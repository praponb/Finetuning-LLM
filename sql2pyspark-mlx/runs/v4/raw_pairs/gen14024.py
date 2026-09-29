from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").desc_nulls_last())
w2 = Window.partitionBy("currency")
w3 = Window.partitionBy("plan").orderBy(F.col("tier_value").desc())
w4 = Window.partitionBy("plan", "currency")

lines = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("discounts").alias("tier_value"))
)

result = (
    lines
    .withColumn("tier_value_cume_dist", F.cume_dist().over(w))
    .withColumn("currency_stddev_samp_tier_value", F.stddev_samp("tier_value").over(w2))
    .withColumn("tier_value_percent_rank", F.percent_rank().over(w3))
    .withColumn("plan_count_tier_value", F.count("tier_value").over(w4))
    .withColumn("plan_avg_tier_value", F.avg("tier_value").over(w4))
)
