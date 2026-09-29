from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("currency").orderBy(F.col("val").desc_nulls_first())
w3 = Window.partitionBy("plan", "currency").orderBy(F.col("val").desc())
w4 = Window.orderBy(F.col("val").desc_nulls_last())
w5 = Window.partitionBy("currency").orderBy(F.col("val").asc_nulls_last())

exploded = (
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
    exploded
    .select(
        "id",
        "plan",
        F.max("val").over(w).alias("plan_max_val"),
        F.percent_rank().over(w2).alias("val_percent_rank"),
        F.cume_dist().over(w3).alias("val_cume_dist"),
        F.cume_dist().over(w4).alias("val_cume_dist_2"),
        F.percent_rank().over(w5).alias("val_percent_rank_2"),
    )
)
