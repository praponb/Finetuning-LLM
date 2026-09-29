from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("val")
w2 = Window.orderBy(F.col("val").asc_nulls_last())
w3 = Window.orderBy(F.col("val").desc_nulls_first())
w4 = Window.orderBy(F.col("val").asc_nulls_first())
w_running = Window.orderBy(F.col("val_percent_rank_4").desc())
w6 = Window.orderBy("val_percent_rank_4")
w7 = Window.orderBy(F.col("val_percent_rank_4_percent_rank").desc_nulls_first())
w8 = Window.orderBy(F.col("val_percent_rank_2").asc_nulls_first())
w9 = Window.orderBy(F.col("val_percent_rank_3").desc_nulls_last())
w10 = Window.orderBy(F.col("val_percent_rank_2_per_val").desc_nulls_last())

flattened = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("discounts").alias("val"))
)

scored = (
    flattened
    .withColumn("val_percent_rank", F.percent_rank().over(w))
    .withColumn("val_percent_rank_2", F.percent_rank().over(w2))
    .withColumn("val_percent_rank_3", F.percent_rank().over(w3))
    .withColumn("val_cume_dist", F.cume_dist().over(w4))
    .withColumn("val_percent_rank_4", F.percent_rank().over(w3))
)

base = (
    scored
    .filter(F.col("effective_from") > F.lit("2024-02-20").cast("date"))
    .select(
        "effective_from",
        "val",
        "val_percent_rank_2",
        "val_percent_rank_3",
        "val_cume_dist",
        "val_percent_rank_4",
        F.coalesce("val", F.lit(0)).alias("val_filled"),
        (F.col("val_percent_rank_2") / F.nullif("val", F.lit(0))).alias("val_percent_rank_2_per_val"),
    )
)

enriched = (
    base
    .select(
        "val",
        "val_percent_rank_2",
        "val_percent_rank_3",
        "val_cume_dist",
        "val_percent_rank_2_per_val",
        F.cume_dist().over(w_running).alias("val_percent_rank_4_cume_dist"),
        F.percent_rank().over(w6).alias("val_percent_rank_4_percent_rank"),
    )
)

enriched_price_tiers = (
    enriched
    .select(
        "*",
        F.percent_rank().over(w7).alias("val_percent_rank_4_percent_rank_percent_rank"),
        F.dense_rank().over(w8).alias("val_percent_rank_2_dense_rank"),
        F.rank().over(w9).alias("val_percent_rank_3_rank"),
        F.dense_rank().over(w10).alias("val_percent_rank_2_per_val_dense_rank"),
    )
)

cleaned = (
    enriched_price_tiers
    .filter(F.col("val_percent_rank_3_rank") != 5)
    .withColumn("val_percent_rank_2_times_val_percent_rank_3", (F.col("val_percent_rank_2") * F.col("val_percent_rank_3")))
    .withColumn("val_percent_rank_3_rank_plus_val_percent_rank_4_percent_rank", (F.col("val_percent_rank_3_rank") + F.col("val_percent_rank_4_percent_rank")))
)

base_price_tiers = (
    cleaned
    .filter(F.col("val") <= 62)
    .select(
        "*",
        F.col("val_percent_rank_3").cast("int").alias("val_percent_rank_3_int"),
    )
)

result = (
    base_price_tiers
    .select(
        "val",
        "val_percent_rank_2",
        "val_percent_rank_3",
        "val_percent_rank_2_per_val",
        "val_percent_rank_4_cume_dist",
        "val_percent_rank_4_percent_rank_percent_rank",
        "val_percent_rank_2_dense_rank",
        "val_percent_rank_3_rank",
        "val_percent_rank_2_per_val_dense_rank",
    )
    .orderBy(F.col("val_percent_rank_2").desc_nulls_last())
)
