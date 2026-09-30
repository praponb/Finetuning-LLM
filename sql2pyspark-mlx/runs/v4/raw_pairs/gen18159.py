from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.partitionBy("currency").orderBy(F.col("element").desc_nulls_first())

lines = (
    df_price_tiers
    .select("id", "currency", F.explode_outer("tier_prices").alias("element"))
)

result = (
    lines
    .select(
        "*",
        F.min("element").over(w).alias("currency_min_element"),
        F.cume_dist().over(w2).alias("element_cume_dist"),
        F.sum("element").over(w).alias("currency_sum_element"),
    )
)
