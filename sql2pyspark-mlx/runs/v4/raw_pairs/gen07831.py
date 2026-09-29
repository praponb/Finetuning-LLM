from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_first())
w2 = Window.orderBy(F.col("tier_value").desc_nulls_first())
w3 = Window.partitionBy("currency").orderBy(F.col("tier_value").asc_nulls_first())

exploded = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("tier_value"),
    )
)

result = (
    exploded
    .withColumn("tier_value_rank", F.rank().over(w))
    .withColumn("tier_value_dense_rank", F.dense_rank().over(w2))
    .withColumn("tier_value_rank_2", F.rank().over(w3))
)
