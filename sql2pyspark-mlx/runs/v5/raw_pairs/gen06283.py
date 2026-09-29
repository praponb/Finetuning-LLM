from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w_recent = Window.partitionBy("plan").orderBy(F.col("tier_value").desc_nulls_last())

lines = df_price_tiers.select("id", "plan", F.explode("tier_limits").alias("tier_value"))

result = (
    lines
    .filter(F.col("tier_value").isNull())
    .select(
        "id",
        "plan",
        F.stddev_samp("tier_value").over(w).alias("plan_stddev_samp_tier_value"),
        F.percent_rank().over(w_recent).alias("tier_value_percent_rank"),
    )
)
