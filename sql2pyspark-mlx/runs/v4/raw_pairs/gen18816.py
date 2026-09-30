from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("element").desc_nulls_last())
w2 = Window.partitionBy("plan", "currency").orderBy(F.col("element").desc_nulls_first())
w3 = Window.partitionBy("currency")

flattened = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_limits").alias("element"))
)

result = (
    flattened
    .withColumn("element_rank", F.rank().over(w))
    .withColumn("element_rank_2", F.rank().over(w2))
    .withColumn("currency_avg_element", F.avg("element").over(w3))
)
