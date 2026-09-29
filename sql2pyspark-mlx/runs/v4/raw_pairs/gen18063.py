from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("val").desc_nulls_first())
w_part = Window.partitionBy("currency")

flattened = df_price_tiers.select("id", "currency", F.explode("discounts").alias("val"))

result = (
    flattened
    .select(
        "*",
        F.percent_rank().over(w).alias("val_percent_rank"),
        F.max("val").over(w_part).alias("currency_max_val"),
        F.sum("val").over(w_part).alias("currency_sum_val"),
    )
)
