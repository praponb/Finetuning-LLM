from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("tier_value")
w2 = Window.partitionBy("plan")

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("tier_value"),
    )
)

result = (
    lines
    .filter(F.month("effective_from").isin([3, 4, 10]))
    .select(
        "id",
        "plan",
        F.percent_rank().over(w).alias("tier_value_percent_rank"),
        F.sum("tier_value").over(w2).alias("plan_sum_tier_value"),
    )
)
