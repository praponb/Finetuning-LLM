from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("val").desc_nulls_last())
w2 = Window.partitionBy("currency").orderBy(F.col("val").desc_nulls_last())
w3 = Window.orderBy(F.col("val").desc())
w4 = Window.partitionBy("currency")
w5 = Window.partitionBy("currency").orderBy(F.col("val").asc_nulls_last())
w6 = Window.partitionBy("currency").orderBy(F.col("val_rank").asc_nulls_last())
w7 = Window.orderBy(F.col("currency_sum_val_dense_rank_2").desc_nulls_first())
w_running = Window.orderBy(F.col("val_dense_rank_2").desc())
w9 = Window.orderBy(F.col("val_dense_rank_2").asc_nulls_first())

lines = df_price_tiers.select("id", "currency", F.explode("discounts").alias("val"))

metrics = (
    lines
    .filter(F.col("val").between(25, 62))
    .select(
        "id",
        "currency",
        "val",
        F.dense_rank().over(w).alias("val_dense_rank"),
        F.dense_rank().over(w2).alias("val_dense_rank_2"),
        F.rank().over(w3).alias("val_rank"),
        F.max("val").over(w4).alias("currency_max_val"),
        F.percent_rank().over(w5).alias("val_percent_rank"),
    )
)

metrics_price_tiers = (
    metrics
    .select(
        "*",
        F.rank().over(w6).alias("val_rank_rank"),
        F.sum("val_dense_rank_2").over(w4).alias("currency_sum_val_dense_rank_2"),
    )
)

cleaned = (
    metrics_price_tiers
    .filter(F.col("val_dense_rank_2").between(4, 7))
    .select(
        "id",
        "val",
        "val_dense_rank_2",
        "val_rank",
        "val_percent_rank",
        "val_rank_rank",
        "currency_sum_val_dense_rank_2",
        F.floor("val_percent_rank").alias("val_percent_rank_whole"),
    )
)

scored = (
    cleaned
    .withColumn("currency_sum_val_dense_rank_2_cume_dist", F.cume_dist().over(w7))
    .withColumn("val_dense_rank_2_percent_rank", F.percent_rank().over(w_running))
    .withColumn("val_dense_rank_2_percent_rank_2", F.percent_rank().over(w9))
)

valid = (
    scored
    .filter(F.col("currency_sum_val_dense_rank_2_cume_dist").between(0.24, 0.33))
    .withColumn("currency_sum_val_dense_rank_2_cume_dist_level", F.when(F.col("currency_sum_val_dense_rank_2_cume_dist") >= 0.85, "High").when(F.col("currency_sum_val_dense_rank_2_cume_dist") >= 0.26, "Medium").otherwise("Low"))
)

result = (
    valid
    .groupBy(
        F.when(F.col("val_percent_rank_whole") >= 83, "HIGH").otherwise("LOW").alias("val_percent_rank_whole_tier"),
        F.when(F.col("val_dense_rank_2_percent_rank") >= 0.71, "HIGH").otherwise("LOW").alias("val_dense_rank_2_percent_rank_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.min("currency_sum_val_dense_rank_2_cume_dist").alias("min_currency_sum_val_dense_rank_2_cume_dist"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
    .orderBy(F.col("val_percent_rank_whole_tier").desc_nulls_last())
)
