from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("tier_value")
w_ordered = Window.partitionBy("plan")
w_recent = Window.partitionBy("plan").orderBy(F.col("tier_value").desc_nulls_first())

exploded = df_price_tiers.select("id", "plan", F.explode("discounts").alias("tier_value"))

result = (
    exploded
    .select(
        "*",
        F.percent_rank().over(w).alias("tier_value_percent_rank"),
        F.min("tier_value").over(w_ordered).alias("plan_min_tier_value"),
        F.dense_rank().over(w_recent).alias("tier_value_dense_rank"),
    )
)
