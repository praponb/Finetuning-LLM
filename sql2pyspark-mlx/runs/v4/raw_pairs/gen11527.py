from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("element").asc_nulls_first())
w2 = Window.partitionBy("plan")
w3 = Window.partitionBy("plan").orderBy(F.col("element").desc_nulls_last())

exploded = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("tier_limits").alias("element"))
)

result = (
    exploded
    .withColumn("element_cume_dist", F.cume_dist().over(w))
    .withColumn("plan_max_element", F.max("element").over(w2))
    .withColumn("element_dense_rank", F.dense_rank().over(w3))
)
