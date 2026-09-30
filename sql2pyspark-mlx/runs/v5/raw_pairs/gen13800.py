from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency", "plan").orderBy(F.col("val").desc_nulls_first())
w2 = Window.partitionBy("plan").orderBy(F.col("val").desc_nulls_last())
w3 = Window.partitionBy("plan").orderBy(F.col("val").desc())

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("discounts").alias("val"),
    )
)

result = (
    exploded
    .filter(F.col("currency").isin(["USD", "THB", "EUR"]))
    .select(
        "id",
        "plan",
        F.cume_dist().over(w).alias("val_cume_dist"),
        F.percent_rank().over(w2).alias("val_percent_rank"),
        F.dense_rank().over(w3).alias("val_dense_rank"),
    )
)
