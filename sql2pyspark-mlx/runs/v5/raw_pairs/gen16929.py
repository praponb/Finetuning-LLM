from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy(F.col("val_whole").asc_nulls_first())
w2 = Window.orderBy("val")
w_running = Window.partitionBy("currency").orderBy(F.col("val_whole").asc_nulls_last())
w_part = Window.partitionBy("currency").orderBy(F.col("val_whole").desc_nulls_last())
w5 = Window.partitionBy("currency").orderBy("val")
w6 = Window.orderBy(F.col("val_percent_rank_2").desc_nulls_last())
w7 = Window.orderBy("val_whole")
w8 = Window.orderBy(F.col("val_whole").asc_nulls_last())

exploded = df_price_tiers.select("id", "currency", F.explode("tier_limits").alias("val"))

base = (
    exploded
    .filter(F.col("currency").isin(["EUR", "USD"]))
    .withColumn("currency_upper", F.upper("currency"))
    .withColumn("val_whole", F.floor("val"))
)

remaining_keys = (
    base
    .select("currency")
    .subtract(
        df_price_tiers
        .filter(F.col("effective_from") >= "2023-11-21")
        .select("currency")
    )
)

reconciled = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.currency") == F.col("k.currency")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .filter(F.col("val") <= 46)
    .select(
        "id",
        "val",
        "val_whole",
        F.rank().over(w_grp).alias("val_whole_rank"),
        F.percent_rank().over(w2).alias("val_percent_rank"),
        F.rank().over(w_running).alias("val_whole_rank_2"),
        F.rank().over(w_part).alias("val_whole_rank_3"),
        F.percent_rank().over(w5).alias("val_percent_rank_2"),
    )
)

result = (
    with_windows
    .select(
        "val_whole",
        "val_percent_rank",
        "val_whole_rank_3",
        "val_percent_rank_2",
        F.rank().over(w6).alias("val_percent_rank_2_rank"),
        F.dense_rank().over(w7).alias("val_whole_dense_rank"),
        F.dense_rank().over(w8).alias("val_whole_dense_rank_2"),
    )
)
