from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").desc_nulls_last())
w2 = Window.partitionBy("currency").orderBy("tier_value")
w_hist = Window.partitionBy("currency")

lines = (
    df_price_tiers
    .select("id", "currency", F.explode("tier_prices").alias("tier_value"))
)

result = (
    lines
    .withColumn("tier_value_cume_dist", F.cume_dist().over(w))
    .withColumn("tier_value_dense_rank", F.dense_rank().over(w2))
    .withColumn("currency_sum_tier_value", F.sum("tier_value").over(w_hist))
    .withColumn("currency_stddev_samp_tier_value", F.stddev_samp("tier_value").over(w_hist))
)
