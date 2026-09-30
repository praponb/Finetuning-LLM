from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.partitionBy("currency").orderBy(F.col("val").desc_nulls_last())
w_grp = Window.partitionBy("currency").orderBy(F.col("val_int").desc())
w_running = Window.partitionBy("currency").orderBy(F.col("val").desc_nulls_first())
w5 = Window.partitionBy("currency").orderBy("val")
w6 = Window.orderBy(F.col("currency_avg_currency_max_val_int_times_val").desc_nulls_first())
w7 = Window.orderBy(F.col("val_dense_rank").desc())
w_recent = Window.orderBy("val_dense_rank_2")
w_recent_2 = Window.orderBy(F.col("val_int_times_val").desc_nulls_last())

exploded = df_price_tiers.select("id", "currency", F.explode_outer("discounts").alias("val"))

filtered = (
    exploded
    .filter(F.col("currency") == "EUR")
    .withColumn("val_int", F.col("val").cast("int"))
)

filtered_price_tiers = (
    filtered
    .filter(F.col("currency") != "EUR")
    .withColumn("val_int_times_val", (F.col("val_int") * F.col("val")))
)

scored = (
    filtered_price_tiers
    .withColumn("currency_max_val_int_times_val", F.max("val_int_times_val").over(w))
    .withColumn("val_cume_dist", F.cume_dist().over(w2))
    .withColumn("val_int_rank", F.rank().over(w_grp))
)

scored_price_tiers = (
    scored
    .withColumn("currency_avg_currency_max_val_int_times_val", F.avg("currency_max_val_int_times_val").over(w))
    .withColumn("currency_stddev_samp_val_int", F.stddev_samp("val_int").over(w))
)

with_windows = (
    scored_price_tiers
    .select(
        "id",
        "val_int_times_val",
        "currency_max_val_int_times_val",
        "val_cume_dist",
        "currency_avg_currency_max_val_int_times_val",
        "currency_stddev_samp_val_int",
        F.dense_rank().over(w_running).alias("val_dense_rank"),
        F.dense_rank().over(w5).alias("val_dense_rank_2"),
    )
)

result = (
    with_windows
    .select(
        "val_cume_dist",
        "currency_avg_currency_max_val_int_times_val",
        "val_dense_rank",
        "val_dense_rank_2",
        F.dense_rank().over(w6).alias("currency_avg_currency_max_val_int_times_val_dense_rank"),
        F.dense_rank().over(w7).alias("val_dense_rank_dense_rank"),
        F.percent_rank().over(w_recent).alias("val_dense_rank_2_percent_rank"),
        F.cume_dist().over(w_recent).alias("val_dense_rank_2_cume_dist"),
        F.cume_dist().over(w_recent_2).alias("val_int_times_val_cume_dist"),
    )
)
