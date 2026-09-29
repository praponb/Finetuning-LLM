from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").desc())
w2 = Window.partitionBy("plan").orderBy(F.col("tier_value").asc_nulls_last())

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode("tier_limits").alias("tier_value"),
    )
)

result = (
    exploded
    .withColumn("tier_value_percent_rank", F.percent_rank().over(w))
    .withColumn("tier_value_rank", F.rank().over(w2))
)
