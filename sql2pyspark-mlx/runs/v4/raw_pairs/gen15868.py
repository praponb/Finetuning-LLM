from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("tier_value").desc_nulls_last())
w2 = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_first())

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "currency",
        F.explode_outer("tier_limits").alias("tier_value"),
    )
)

result = (
    exploded
    .withColumn("tier_value_rank", F.rank().over(w))
    .withColumn("tier_value_dense_rank", F.dense_rank().over(w2))
)
