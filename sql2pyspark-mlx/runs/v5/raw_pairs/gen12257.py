from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy(F.col("element").desc())

lines = df_price_tiers.select("id", "plan", F.explode_outer("tier_prices").alias("element"))

result = (
    lines
    .select(
        "id",
        "plan",
        F.sum("element").over(w).alias("plan_sum_element"),
        F.cume_dist().over(w2).alias("element_cume_dist"),
        F.max("element").over(w).alias("plan_max_element"),
    )
)
