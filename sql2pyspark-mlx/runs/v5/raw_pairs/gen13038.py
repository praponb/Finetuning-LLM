from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").desc_nulls_last())
w2 = Window.partitionBy("plan").orderBy(F.col("tier_value").asc_nulls_last())
w3 = Window.partitionBy("plan")

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode("tier_limits").alias("tier_value"),
    )
)

result = (
    lines
    .withColumn("tier_value_dense_rank", F.dense_rank().over(w))
    .withColumn("tier_value_cume_dist", F.cume_dist().over(w2))
    .withColumn("plan_stddev_samp_tier_value", F.stddev_samp("tier_value").over(w3))
)
