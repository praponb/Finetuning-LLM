from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("currency")
w2 = Window.orderBy(F.col("val").desc_nulls_first())
w3 = Window.orderBy(F.col("val").desc_nulls_last())

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("val"),
    )
)

result = (
    exploded
    .select(
        "*",
        F.avg("val").over(w_ordered).alias("currency_avg_val"),
        F.rank().over(w2).alias("val_rank"),
        F.percent_rank().over(w3).alias("val_percent_rank"),
    )
)
