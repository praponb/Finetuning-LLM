from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy("tier_value")
w2 = Window.orderBy(F.col("tier_value").desc_nulls_last())

flattened = df_price_tiers.select("id", F.explode("tier_limits").alias("tier_value"))

base = (
    flattened
    .filter(F.col("tier_value").between(23, 79))
    .select(
        "*",
        F.round("tier_value", 1).alias("tier_value_rounded"),
        F.col("tier_value").cast("string").alias("tier_value_str"),
    )
)

metrics = (
    base
    .select(
        "id",
        "tier_value",
        "tier_value_rounded",
        F.rank().over(w_hist).alias("tier_value_rank"),
    )
)

enriched = metrics.withColumn("tier_value_percent_rank", F.percent_rank().over(w2))

result = (
    enriched
    .select("id", "tier_value_rank")
    .orderBy(F.col("id").desc())
)
