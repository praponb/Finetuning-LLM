from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("plan")

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode("tier_prices").alias("tier_value"),
    )
)

result = (
    lines
    .withColumn("plan_count_tier_value", F.count("tier_value").over(w_running))
    .withColumn("plan_avg_tier_value", F.avg("tier_value").over(w_running))
    .withColumn("plan_avg_tier_value_2", F.avg("tier_value").over(w_running))
)
