from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy("tier_value")
w2 = Window.partitionBy("plan", "currency").orderBy(F.col("tier_value").asc_nulls_first())
w3 = Window.partitionBy("currency").orderBy(F.col("tier_value").desc())
w4 = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_first())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("tier_value"),
    )
)

result = (
    lines
    .withColumn("tier_value_dense_rank", F.dense_rank().over(w))
    .withColumn("tier_value_rank", F.rank().over(w2))
    .withColumn("tier_value_percent_rank", F.percent_rank().over(w))
    .withColumn("tier_value_dense_rank_2", F.dense_rank().over(w3))
    .withColumn("tier_value_dense_rank_3", F.dense_rank().over(w4))
)
