from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("val")
w2 = Window.orderBy(F.col("val").asc_nulls_last())
w3 = Window.orderBy(F.col("val_cume_dist").desc_nulls_last())
w4 = Window.orderBy(F.col("val_percent_rank").desc_nulls_last())
w5 = Window.orderBy(F.col("val_cume_dist_percent_rank").desc())

exploded = df_price_tiers.select("id", F.explode_outer("tier_prices").alias("val"))

metrics = (
    exploded
    .withColumn("val_percent_rank", F.percent_rank().over(w))
    .withColumn("val_percent_rank_2", F.percent_rank().over(w2))
    .withColumn("val_cume_dist", F.cume_dist().over(w2))
)

enriched = (
    metrics
    .select(
        "*",
        F.rank().over(w3).alias("val_cume_dist_rank"),
        F.percent_rank().over(w3).alias("val_cume_dist_percent_rank"),
        F.rank().over(w4).alias("val_percent_rank_rank"),
    )
)

scored = (
    enriched
    .withColumn("val_percent_rank_3", F.percent_rank().over(w))
    .withColumn("val_cume_dist_percent_rank_dense_rank", F.dense_rank().over(w5))
)

valid = (
    scored
    .filter(F.col("val_percent_rank_3") > 0.77)
    .select(
        "val",
        "val_percent_rank",
        "val_percent_rank_2",
        "val_cume_dist",
        "val_cume_dist_percent_rank",
        "val_percent_rank_rank",
        "val_percent_rank_3",
        F.round((F.col("val_cume_dist_percent_rank_dense_rank") * 1.1), 2).alias("adjusted_val_cume_dist_percent_rank_dense_rank"),
    )
)

result = (
    valid
    .select(
        "val",
        "val_percent_rank_2",
        "val_cume_dist_percent_rank",
        "val_percent_rank_rank",
    )
)
