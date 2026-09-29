from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.orderBy("tier_value")
w3 = Window.partitionBy("plan")

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("discounts").alias("tier_value"),
    )
)

result = (
    flattened
    .select(
        "id",
        "currency",
        "tier_value",
        F.count("tier_value").over(w).alias("currency_count_tier_value"),
        F.dense_rank().over(w2).alias("tier_value_dense_rank"),
        F.max("tier_value").over(w3).alias("plan_max_tier_value"),
    )
)
