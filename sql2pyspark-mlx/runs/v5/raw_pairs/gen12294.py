from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy(F.col("val_int").desc_nulls_last())
w_hist = Window.orderBy(F.col("val").desc())
w3 = Window.orderBy(F.col("val_int_percent_rank").desc())
w4 = Window.orderBy("val_int_percent_rank")

exploded = df_price_tiers.select("id", F.explode_outer("tier_prices").alias("val"))

base = (
    exploded
    .filter(F.col("val") > 66)
    .withColumn("val_int", F.col("val").cast("int"))
)

enriched = (
    base
    .select(
        "val",
        "val_int",
        F.percent_rank().over(w_grp).alias("val_int_percent_rank"),
        F.dense_rank().over(w_grp).alias("val_int_dense_rank"),
        F.dense_rank().over(w_hist).alias("val_dense_rank"),
    )
)

with_windows = (
    enriched
    .select(
        "val_int",
        "val_int_percent_rank",
        "val_int_dense_rank",
        "val_dense_rank",
        F.percent_rank().over(w3).alias("val_int_percent_rank_percent_rank"),
    )
)

filtered = (
    with_windows
    .filter(F.col("val_int_percent_rank_percent_rank") > 0.34)
    .select(
        "val_int",
        "val_int_percent_rank",
        "val_int_dense_rank",
        "val_dense_rank",
        "val_int_percent_rank_percent_rank",
        (F.col("val_int_percent_rank_percent_rank") / F.nullif("val_int", F.lit(0))).alias("val_int_percent_rank_percent_rank_per_val_int"),
        (F.col("val_int_percent_rank") + F.col("val_dense_rank")).alias("val_int_percent_rank_plus_val_dense_rank"),
    )
)

filtered_price_tiers = (
    filtered
    .filter(F.col("val_int_percent_rank_plus_val_dense_rank") <= 67)
    .select(
        "*",
        F.coalesce("val_int_percent_rank", F.lit(0)).alias("val_int_percent_rank_filled"),
        F.when(F.col("val_int_dense_rank") >= 8, "High").when(F.col("val_int_dense_rank") >= 3, "Medium").otherwise("Low").alias("val_int_dense_rank_level"),
    )
)

with_windows_price_tiers = (
    filtered_price_tiers
    .filter(F.col("val_int_percent_rank_plus_val_dense_rank") < 35)
    .withColumn("val_int_percent_rank_percent_rank_2", F.percent_rank().over(w4))
)

result = (
    with_windows_price_tiers
    .groupBy(
        F.when(F.col("val_int_percent_rank") >= 0.85, "HIGH").otherwise("LOW").alias("val_int_percent_rank_tier"),
        F.when(F.col("val_dense_rank") >= 7, "HIGH").otherwise("LOW").alias("val_dense_rank_tier"),
    )
    .agg(
        F.percentile_approx("val_int", 0.9).alias("val_int_p90"),
        F.percentile_approx("val_int_percent_rank_percent_rank", 0.9).alias("val_int_percent_rank_percent_rank_p90"),
        F.percentile_approx("val_int_dense_rank", [0.25, 0.5, 0.75]).alias("val_int_dense_rank_quartiles"),
    )
    .filter(F.col("val_int_percent_rank_percent_rank_p90") < 1000000)
    .orderBy("val_int_percent_rank_percent_rank_p90")
)
