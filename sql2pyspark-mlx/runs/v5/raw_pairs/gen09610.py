from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w_ordered = Window.orderBy(F.col("val").asc_nulls_first())

flattened = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_limits").alias("val"))
)

result = (
    flattened
    .filter(F.col("currency") != "EUR")
    .select(
        "plan",
        "currency",
        F.count("val").over(w).alias("currency_count_val"),
        F.dense_rank().over(w_ordered).alias("val_dense_rank"),
    )
)
