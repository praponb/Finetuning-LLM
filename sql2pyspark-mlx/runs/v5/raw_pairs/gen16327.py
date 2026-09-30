from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("plan")

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("element"),
    )
)

result = (
    lines
    .select(
        "effective_from",
        "currency",
        F.count("element").over(w_ordered).alias("plan_count_element"),
        F.max("element").over(w_ordered).alias("plan_max_element"),
    )
)
