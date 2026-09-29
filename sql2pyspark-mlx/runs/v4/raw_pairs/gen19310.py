from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").asc_nulls_last())
w2 = Window.orderBy(F.col("tier_value").asc_nulls_first())
w3 = Window.orderBy(F.col("tier_value").desc())

lines = (
    df_price_tiers
    .select("id", "effective_from", F.explode("tier_prices").alias("tier_value"))
)

result = (
    lines
    .withColumn("tier_value_rank", F.rank().over(w))
    .withColumn("tier_value_cume_dist", F.cume_dist().over(w2))
    .withColumn("tier_value_dense_rank", F.dense_rank().over(w))
    .withColumn("tier_value_dense_rank_2", F.dense_rank().over(w3))
)
