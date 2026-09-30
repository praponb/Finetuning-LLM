from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("tier_value").desc())
w2 = Window.partitionBy("plan")

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("tier_value"),
    )
)

result = (
    flattened
    .filter(F.col("effective_from") > "2024-05-16")
    .select(
        "id",
        "currency",
        F.dense_rank().over(w).alias("tier_value_dense_rank"),
        F.max("tier_value").over(w2).alias("plan_max_tier_value"),
    )
)
