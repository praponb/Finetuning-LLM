from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency", "plan")
w2 = Window.partitionBy("currency")

lines = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_prices").alias("tier_value"))
)

result = (
    lines
    .filter(F.col("tier_value").isNotNull())
    .select(
        "id",
        "tier_value",
        F.stddev_samp("tier_value").over(w).alias("currency_stddev_samp_tier_value"),
        F.sum("tier_value").over(w2).alias("currency_sum_tier_value"),
    )
)
