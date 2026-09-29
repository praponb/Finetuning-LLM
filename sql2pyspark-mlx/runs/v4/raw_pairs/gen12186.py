from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency", "plan").orderBy(F.col("val").asc_nulls_last())
w2 = Window.partitionBy("plan").orderBy(F.col("val").desc())
w3 = Window.orderBy(F.col("val").desc_nulls_first())
w_grp = Window.partitionBy("currency")
w5 = Window.partitionBy("currency", "plan").orderBy(F.col("val_percent_rank").asc_nulls_first())
w_recent = Window.orderBy(F.col("currency_stddev_samp_val").desc_nulls_first())
w7 = Window.orderBy(F.col("val_percent_rank").desc())
w8 = Window.orderBy(F.col("val").desc_nulls_last())
w9 = Window.orderBy(F.col("currency_stddev_samp_val_percent_rank").desc_nulls_first())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("val"),
    )
)

cleaned = (
    flattened
    .filter(F.col("val") < 71)
    .withColumn("currency_plan_key", F.concat_ws("-", "currency", "plan"))
)

scored = (
    cleaned
    .withColumn("val_dense_rank", F.dense_rank().over(w))
    .withColumn("val_rank", F.rank().over(w2))
    .withColumn("val_percent_rank", F.percent_rank().over(w3))
    .withColumn("currency_count_val", F.count("val").over(w_grp))
    .withColumn("currency_stddev_samp_val", F.stddev_samp("val").over(w_grp))
)

scored_price_tiers = (
    scored
    .select(
        "*",
        F.cume_dist().over(w5).alias("val_percent_rank_cume_dist"),
        F.percent_rank().over(w_recent).alias("currency_stddev_samp_val_percent_rank"),
        F.min("currency_count_val").over(w_grp).alias("currency_min_currency_count_val"),
    )
)

result = (
    scored_price_tiers
    .withColumn("val_percent_rank_rank", F.rank().over(w7))
    .withColumn("val_rank_2", F.rank().over(w8))
    .withColumn("currency_stddev_samp_val_percent_rank_cume_dist", F.cume_dist().over(w9))
    .withColumn("val_dense_rank_share", F.round((F.col("val_dense_rank") / F.sum("val_dense_rank").over(w_grp)), 4))
    .withColumn("currency_stddev_samp_val_percent_rank_cume_dist_2", F.stddev_samp("val_percent_rank_cume_dist").over(w_grp))
)
