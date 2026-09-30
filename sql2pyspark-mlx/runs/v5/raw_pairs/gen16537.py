from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.orderBy(F.col("element").desc())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("element"),
    )
)

result = (
    lines
    .select(
        "id",
        "plan",
        "effective_from",
        F.count("element").over(w).alias("currency_count_element"),
        F.rank().over(w2).alias("element_rank"),
        F.avg("element").over(w).alias("currency_avg_element"),
    )
)
