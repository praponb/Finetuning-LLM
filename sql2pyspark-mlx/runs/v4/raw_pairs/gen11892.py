from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy("val")
w2 = Window.orderBy(F.col("val").asc_nulls_first())
w_ordered = Window.partitionBy("currency").orderBy(F.col("val").desc())
w4 = Window.partitionBy("currency").orderBy(F.col("val_cume_dist").desc_nulls_last())

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

cleaned = (
    exploded
    .filter(~F.col("currency").isin(["USD", "EUR"]))
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "val",
        F.initcap(F.trim("plan")).alias("plan_title"),
    )
)

with_windows = (
    cleaned
    .withColumn("val_dense_rank", F.dense_rank().over(w))
    .withColumn("val_cume_dist", F.cume_dist().over(w2))
    .withColumn("val_percent_rank", F.percent_rank().over(w_ordered))
)

base = (
    with_windows
    .filter(F.col("val_cume_dist") > 0.8)
    .select("*", F.col("val_dense_rank").cast("double").alias("val_dense_rank_num"))
)

result = (
    base
    .select(
        "*",
        F.rank().over(w2).alias("val_rank"),
        F.percent_rank().over(w4).alias("val_cume_dist_percent_rank"),
    )
)
