from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").desc_nulls_first())
w2 = Window.orderBy("tier_value")
w3 = Window.partitionBy("plan")

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("discounts").alias("tier_value"),
    )
)

result = (
    exploded
    .withColumn("tier_value_percent_rank", F.percent_rank().over(w))
    .withColumn("tier_value_dense_rank", F.dense_rank().over(w2))
    .withColumn("plan_min_tier_value", F.min("tier_value").over(w3))
)
