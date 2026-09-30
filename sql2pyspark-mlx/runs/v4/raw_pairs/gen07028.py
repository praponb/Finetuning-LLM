from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("currency").orderBy(F.col("val").desc_nulls_last())
w2 = Window.partitionBy("plan", "currency")
w3 = Window.partitionBy("currency", "plan")

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("discounts").alias("val"),
    )
)

result = (
    lines
    .select(
        "*",
        F.dense_rank().over(w_part).alias("val_dense_rank"),
        F.min("val").over(w2).alias("plan_min_val"),
        F.count("val").over(w3).alias("currency_count_val"),
    )
)
