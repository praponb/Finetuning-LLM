from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")

lines = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("element"),
    )
)

result = (
    lines
    .select(
        "id",
        "effective_from",
        F.min("element").over(w).alias("currency_min_element"),
    )
)
