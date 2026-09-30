from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("tier_value").asc_nulls_first())

flattened = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_limits").alias("tier_value"))
)

result = (
    flattened
    .select(
        "id",
        "plan",
        "tier_value",
        F.cume_dist().over(w).alias("tier_value_cume_dist"),
    )
)
