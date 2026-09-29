from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("plan")

lines = df_price_tiers.select("id", "plan", F.explode("tier_limits").alias("tier_value"))

result = (
    lines
    .filter(F.col("plan") == "starter")
    .select(
        "id",
        "plan",
        F.sum("tier_value").over(w_ordered).alias("plan_sum_tier_value"),
        F.min("tier_value").over(w_ordered).alias("plan_min_tier_value"),
    )
)
