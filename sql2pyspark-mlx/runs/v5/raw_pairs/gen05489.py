from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").desc_nulls_first())
w_running = Window.partitionBy("plan")

exploded = (
    df_price_tiers
    .select("id", "plan", F.explode_outer("tier_limits").alias("tier_value"))
)

result = (
    exploded
    .select(
        "id",
        "tier_value",
        F.percent_rank().over(w).alias("tier_value_percent_rank"),
        F.max("tier_value").over(w_running).alias("plan_max_tier_value"),
        F.count("tier_value").over(w_running).alias("plan_count_tier_value"),
    )
)
