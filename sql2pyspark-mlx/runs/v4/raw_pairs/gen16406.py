from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("tier_value")
w2 = Window.orderBy(F.col("tier_value").desc_nulls_last())

lines = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("tier_prices").alias("tier_value"))
)

result = (
    lines
    .withColumn("tier_value_rank", F.rank().over(w))
    .withColumn("tier_value_percent_rank", F.percent_rank().over(w2))
    .withColumn("tier_value_rank_2", F.rank().over(w2))
)
