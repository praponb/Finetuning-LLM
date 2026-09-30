from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_first())
w2 = Window.partitionBy("currency").orderBy(F.col("tier_value").asc_nulls_first())

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
        "id",
        "tier_value",
        F.rank().over(w).alias("tier_value_rank"),
        F.percent_rank().over(w2).alias("tier_value_percent_rank"),
    )
)
