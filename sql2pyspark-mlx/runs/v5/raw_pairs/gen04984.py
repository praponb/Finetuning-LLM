from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("val_rounded").desc_nulls_last())
w2 = Window.partitionBy("currency").orderBy(F.col("val_rounded_cume_dist").asc_nulls_first())
w3 = Window.partitionBy("currency").orderBy(F.col("val_rounded").desc_nulls_first())
w4 = Window.partitionBy("currency").orderBy(F.col("val_rounded_cume_dist").desc())
w_recent = Window.partitionBy("currency")

lines = df_price_tiers.select("id", "currency", F.explode_outer("discounts").alias("val"))

base = (
    lines
    .filter(F.col("currency").isin(["THB", "USD"]))
    .select(
        "*",
        F.when(F.col("val") >= 66, "High").when(F.col("val") >= 33, "Medium").otherwise("Low").alias("val_tier"),
    )
)

cleaned = (
    base
    .filter(F.col("currency") != "EUR")
    .withColumn("val_rounded", F.round("val", 0))
    .withColumn("currency_group", F.when(F.col("currency").isin(["USD", "EUR"]), "Group A").otherwise("Other"))
)

valid = (
    cleaned
    .filter(F.col("val") <= 27)
    .withColumn("val_times_val_rounded", (F.col("val") * F.col("val_rounded")))
)

with_windows = valid.select("*", F.cume_dist().over(w).alias("val_rounded_cume_dist"))

result = (
    with_windows
    .filter(F.col("val_rounded_cume_dist") < 0.57)
    .select(
        "val",
        "val_rounded",
        "currency_group",
        "val_times_val_rounded",
        "val_rounded_cume_dist",
        F.cume_dist().over(w2).alias("val_rounded_cume_dist_cume_dist"),
        F.rank().over(w3).alias("val_rounded_rank"),
        F.percent_rank().over(w4).alias("val_rounded_cume_dist_percent_rank"),
        F.sum("val_rounded_cume_dist").over(w_recent).alias("currency_sum_val_rounded_cume_dist"),
    )
)
