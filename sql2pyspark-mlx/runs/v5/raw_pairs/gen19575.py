from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("val").desc())
w2 = Window.orderBy(F.col("val").asc_nulls_first())
w3 = Window.orderBy(F.col("val").desc_nulls_first())
w4 = Window.orderBy(F.col("val").asc_nulls_last())

lines = df_price_tiers.select("id", "effective_from", F.explode("tier_prices").alias("val"))

result = (
    lines
    .select(
        "id",
        "val",
        F.percent_rank().over(w).alias("val_percent_rank"),
        F.rank().over(w2).alias("val_rank"),
        F.rank().over(w3).alias("val_rank_2"),
        F.percent_rank().over(w4).alias("val_percent_rank_2"),
        F.rank().over(w3).alias("val_rank_3"),
    )
)
