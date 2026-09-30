from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").asc_nulls_first())
w_grp = Window.orderBy(F.col("tier_value").desc_nulls_last())

flattened = df_price_tiers.select("id", F.explode("tier_limits").alias("tier_value"))

result = (
    flattened
    .select(
        "id",
        "tier_value",
        F.percent_rank().over(w).alias("tier_value_percent_rank"),
        F.dense_rank().over(w_grp).alias("tier_value_dense_rank"),
    )
)
