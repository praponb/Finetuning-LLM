from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("val").asc_nulls_last())
w2 = Window.partitionBy("plan").orderBy(F.col("val").desc_nulls_last())
w3 = Window.partitionBy("currency").orderBy(F.col("val_whole").desc_nulls_first())
w4 = Window.orderBy(F.col("val").asc_nulls_first())
w5 = Window.partitionBy("currency").orderBy(F.col("val_dense_rank").asc_nulls_last())
w6 = Window.partitionBy("plan")
w7 = Window.partitionBy("currency").orderBy(F.col("val_whole_cume_dist").desc_nulls_first())
w8 = Window.partitionBy("plan").orderBy(F.col("val_whole_cume_dist").desc())
w9 = Window.partitionBy("currency", "plan")
w10 = Window.partitionBy("plan").orderBy(F.col("val").desc_nulls_first())

lines = df_price_tiers.select("id", "plan", "currency", F.explode("discounts").alias("val"))

cleaned = (
    lines
    .filter(F.col("plan") != "pro")
    .select(
        "*",
        F.ceil("val").alias("val_whole"),
        F.when(F.col("plan").isin(["basic", "pro"]), "Core").otherwise("Other").alias("plan_group"),
    )
)

enriched = (
    cleaned
    .select(
        "*",
        F.cume_dist().over(w).alias("val_cume_dist"),
        F.dense_rank().over(w2).alias("val_dense_rank"),
    )
)

enriched_price_tiers = (
    enriched
    .select(
        "plan",
        "currency",
        "val",
        "val_whole",
        "plan_group",
        "val_cume_dist",
        "val_dense_rank",
        F.cume_dist().over(w3).alias("val_whole_cume_dist"),
        F.cume_dist().over(w4).alias("val_cume_dist_2"),
        F.dense_rank().over(w5).alias("val_dense_rank_dense_rank"),
    )
)

result = (
    enriched_price_tiers
    .select(
        "plan",
        "val",
        "val_whole",
        "val_cume_dist",
        "val_dense_rank",
        "val_cume_dist_2",
        F.round((F.col("val_dense_rank") / F.sum("val_dense_rank").over(w6)), 4).alias("val_dense_rank_share"),
        F.percent_rank().over(w7).alias("val_whole_cume_dist_percent_rank"),
        F.cume_dist().over(w8).alias("val_whole_cume_dist_cume_dist"),
        F.min("val_whole_cume_dist").over(w9).alias("currency_min_val_whole_cume_dist"),
        F.percent_rank().over(w10).alias("val_percent_rank"),
    )
)
