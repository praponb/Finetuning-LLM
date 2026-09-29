from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("val").desc_nulls_last())
w2 = Window.partitionBy("plan").orderBy(F.col("val").asc_nulls_first())
w3 = Window.orderBy(F.col("val").desc())
w4 = Window.partitionBy("plan", "currency").orderBy(F.col("val").desc_nulls_first())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("val"),
    )
)

with_windows = (
    lines
    .filter(F.col("plan") == "starter")
    .select(
        "id",
        "plan",
        F.cume_dist().over(w).alias("val_cume_dist"),
        F.rank().over(w2).alias("val_rank"),
        F.percent_rank().over(w3).alias("val_percent_rank"),
        F.rank().over(w4).alias("val_rank_2"),
        F.dense_rank().over(w4).alias("val_dense_rank"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("val_dense_rank") < 7)
    .select(
        "id",
        "plan",
        "val_rank",
        "val_rank_2",
        "val_dense_rank",
        F.col("val_rank_2").cast("string").alias("val_rank_2_str"),
    )
)

filtered = (
    cleaned
    .filter(F.col("plan") != "basic")
    .select(
        "id",
        "plan",
        "val_rank_2",
        "val_dense_rank",
        "val_rank_2_str",
        (F.col("val_rank_2") + F.col("val_dense_rank")).alias("val_rank_2_plus_val_dense_rank"),
    )
)

result = (
    filtered
    .groupBy("plan")
    .agg(
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("plan") == "pro", 1)).alias("pro_count"),
    )
    .filter(F.col("num_rows") > 1)
    .orderBy(F.col("plan").asc_nulls_first())
)
