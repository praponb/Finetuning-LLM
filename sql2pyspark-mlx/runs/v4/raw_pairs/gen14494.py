from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency", "plan")
w2 = Window.partitionBy("currency").orderBy(F.col("val").desc())
w3 = Window.partitionBy("plan").orderBy("val")

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("val"),
    )
)

result = (
    flattened
    .select(
        "*",
        F.count("val").over(w).alias("currency_count_val"),
        F.rank().over(w2).alias("val_rank"),
        F.rank().over(w3).alias("val_rank_2"),
    )
)
