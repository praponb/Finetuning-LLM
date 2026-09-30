from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").desc())
w2 = Window.orderBy(F.col("tier_value").desc_nulls_last())
w3 = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_last())

lines = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_limits").alias("tier_value"))
)

result = (
    lines
    .withColumn("tier_value_cume_dist", F.cume_dist().over(w))
    .withColumn("tier_value_percent_rank", F.percent_rank().over(w2))
    .withColumn("tier_value_dense_rank", F.dense_rank().over(w3))
)
