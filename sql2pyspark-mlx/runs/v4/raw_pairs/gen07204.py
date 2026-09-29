from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("val")
w2 = Window.orderBy(F.col("val").desc())
w3 = Window.partitionBy("plan")
w4 = Window.partitionBy("plan").orderBy(F.col("val").desc_nulls_first())
w5 = Window.partitionBy("plan").orderBy(F.col("val").desc_nulls_last())

exploded = df_price_tiers.select("id", "plan", F.explode("discounts").alias("val"))

metrics = (
    exploded
    .select(
        "*",
        F.percent_rank().over(w).alias("val_percent_rank"),
        F.percent_rank().over(w2).alias("val_percent_rank_2"),
        F.min("val").over(w3).alias("plan_min_val"),
        F.dense_rank().over(w4).alias("val_dense_rank"),
        F.rank().over(w5).alias("val_rank"),
    )
)

cleaned = (
    metrics
    .filter(F.col("plan").isin(["basic", "enterprise"]))
    .select(
        "id",
        "plan",
        "val",
        "val_percent_rank",
        "val_percent_rank_2",
        "val_rank",
        (F.col("val_dense_rank") / F.nullif("val_percent_rank", F.lit(0))).alias("val_dense_rank_per_val_percent_rank"),
        (F.col("val_rank") - F.col("val_percent_rank_2")).alias("val_rank_minus_val_percent_rank_2"),
    )
)

result = (
    cleaned
    .select("plan", "val", "val_percent_rank_2", "val_rank")
    .orderBy("val")
)
