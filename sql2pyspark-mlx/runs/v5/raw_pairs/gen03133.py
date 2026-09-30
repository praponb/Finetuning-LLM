from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("currency")
w2 = Window.partitionBy("currency").orderBy(F.col("element").desc())

exploded = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("tier_limits").alias("element"))
)

result = (
    exploded
    .select(
        "id",
        "plan",
        "currency",
        F.min("element").over(w_ordered).alias("currency_min_element"),
        F.rank().over(w2).alias("element_rank"),
    )
)
