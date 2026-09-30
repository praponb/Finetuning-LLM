from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("element").desc_nulls_last())
w2 = Window.partitionBy("currency")
w3 = Window.partitionBy("currency").orderBy(F.col("element").desc())
w4 = Window.partitionBy("currency").orderBy(F.col("element").asc_nulls_last())
w5 = Window.orderBy("element")

exploded = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("element"),
    )
)

result = (
    exploded
    .filter(F.col("currency").isin(["EUR", "USD"]))
    .withColumn("element_cume_dist", F.cume_dist().over(w))
    .withColumn("currency_max_element", F.max("element").over(w2))
    .withColumn("element_cume_dist_2", F.cume_dist().over(w3))
    .withColumn("element_percent_rank", F.percent_rank().over(w4))
    .withColumn("element_dense_rank", F.dense_rank().over(w5))
)
