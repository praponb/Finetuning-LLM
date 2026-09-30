from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("currency").orderBy(F.col("val").desc())

flattened = (
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
    flattened
    .select(
        "plan",
        "currency",
        "val",
        F.sum("val").over(w).alias("plan_sum_val"),
        F.rank().over(w2).alias("val_rank"),
    )
)
