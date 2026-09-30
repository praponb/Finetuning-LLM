from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("tier_value").asc_nulls_first())
w2 = Window.orderBy(F.col("tier_value").desc_nulls_first())

exploded = (
    df_price_tiers
    .select("id", "plan", F.explode_outer("tier_prices").alias("tier_value"))
)

result = (
    exploded
    .withColumn("tier_value_cume_dist", F.cume_dist().over(w))
    .withColumn("tier_value_cume_dist_2", F.cume_dist().over(w2))
)
