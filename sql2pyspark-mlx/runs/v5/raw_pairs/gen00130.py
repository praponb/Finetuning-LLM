from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("tier_value").desc())

flattened = (
    df_price_tiers
    .select("id", "plan", F.explode_outer("discounts").alias("tier_value"))
)

result = (
    flattened
    .select(
        "id",
        "plan",
        F.percent_rank().over(w).alias("tier_value_percent_rank"),
        F.cume_dist().over(w).alias("tier_value_cume_dist"),
        F.dense_rank().over(w).alias("tier_value_dense_rank"),
    )
)
