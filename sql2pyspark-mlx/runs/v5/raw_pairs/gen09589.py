from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("tier_value").desc())

lines = (
    df_price_tiers
    .select("id", "plan", F.explode_outer("tier_prices").alias("tier_value"))
)

result = (
    lines
    .filter(F.col("plan").isin(["enterprise", "basic"]))
    .select("id", "plan", F.percent_rank().over(w).alias("tier_value_percent_rank"))
)
