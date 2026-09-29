from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "currency").orderBy("tier_value")
w2 = Window.partitionBy("plan", "currency")
w3 = Window.partitionBy("currency").orderBy(F.col("tier_value").desc())
w4 = Window.partitionBy("plan", "currency").orderBy(F.col("tier_value").desc())
w5 = Window.partitionBy("currency", "plan").orderBy(F.col("tier_value").desc())

exploded = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("discounts").alias("tier_value"))
)

result = (
    exploded
    .withColumn("tier_value_percent_rank", F.percent_rank().over(w))
    .withColumn("plan_max_tier_value", F.max("tier_value").over(w2))
    .withColumn("tier_value_cume_dist", F.cume_dist().over(w3))
    .withColumn("tier_value_cume_dist_2", F.cume_dist().over(w4))
    .withColumn("tier_value_rank", F.rank().over(w5))
)
