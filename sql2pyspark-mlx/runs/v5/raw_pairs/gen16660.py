from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")

lines = (
    df_price_tiers
    .select("id", "effective_from", "currency", F.explode("discounts").alias("val"))
)

result = (
    lines
    .filter(F.col("currency").isin(["THB", "USD"]))
    .select(
        "id",
        "effective_from",
        "val",
        F.sum("val").over(w).alias("currency_sum_val"),
        F.sum("val").over(w).alias("currency_sum_val_2"),
        F.min("val").over(w).alias("currency_min_val"),
    )
)
