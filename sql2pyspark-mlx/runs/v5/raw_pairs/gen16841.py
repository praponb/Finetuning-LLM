from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("val")
w2 = Window.orderBy(F.col("val").asc_nulls_first())
w3 = Window.partitionBy("currency").orderBy(F.col("val").desc_nulls_first())
w4 = Window.orderBy(F.col("val").desc())
w5 = Window.partitionBy("currency")

lines = df_price_tiers.select("id", "currency", F.explode("tier_prices").alias("val"))

scored = (
    lines
    .withColumn("val_rank", F.rank().over(w))
    .withColumn("val_dense_rank", F.dense_rank().over(w2))
    .withColumn("val_percent_rank", F.percent_rank().over(w3))
    .withColumn("val_rank_2", F.rank().over(w4))
)

with_windows = (
    scored
    .select(
        "*",
        F.round((F.col("val_dense_rank") / F.sum("val_dense_rank").over(w5)), 4).alias("val_dense_rank_share"),
    )
)

filtered = (
    with_windows
    .filter(F.col("val_dense_rank") < 6)
    .select(
        "id",
        "currency",
        "val_rank",
        "val_rank_2",
        "val_dense_rank_share",
        F.when(F.col("val_rank") >= 7, "High").when(F.col("val_rank") >= 4, "Medium").otherwise("Low").alias("val_rank_band"),
    )
)

result = filtered.select("val_dense_rank_share", "val_rank_band")
