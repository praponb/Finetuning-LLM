from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("tier_value").asc_nulls_first())
w2 = Window.partitionBy("currency")
w3 = Window.orderBy(F.col("tier_value").desc_nulls_first())

exploded = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode("discounts").alias("tier_value"),
    )
)

result = (
    exploded
    .select(
        "*",
        F.rank().over(w).alias("tier_value_rank"),
        F.sum("tier_value").over(w2).alias("currency_sum_tier_value"),
        F.percent_rank().over(w3).alias("tier_value_percent_rank"),
    )
)
