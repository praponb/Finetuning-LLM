from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("val").desc_nulls_first())
w2 = Window.partitionBy("plan", "currency").orderBy(F.col("val").desc())
w3 = Window.partitionBy("plan").orderBy(F.col("val").desc())
w4 = Window.partitionBy("plan")
w5 = Window.orderBy(F.col("val_dense_rank_2_share").asc_nulls_last())
w6 = Window.partitionBy("currency", "plan").orderBy("val_percent_rank")

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("val"),
    )
)

metrics = (
    exploded
    .withColumn("val_percent_rank", F.percent_rank().over(w))
    .withColumn("val_dense_rank", F.dense_rank().over(w2))
    .withColumn("val_dense_rank_2", F.dense_rank().over(w3))
)

metrics_price_tiers = (
    metrics
    .filter(F.col("val_percent_rank").between(0.35, 0.49))
    .select(
        "id",
        "plan",
        "currency",
        "val",
        "val_percent_rank",
        F.round((F.col("val_dense_rank_2") / F.sum("val_dense_rank_2").over(w4)), 4).alias("val_dense_rank_2_share"),
    )
)

scored = (
    metrics_price_tiers
    .select(
        "id",
        "currency",
        "val",
        "val_dense_rank_2_share",
        F.max("val_dense_rank_2_share").over(w4).alias("plan_max_val_dense_rank_2_share"),
        F.percent_rank().over(w5).alias("val_dense_rank_2_share_percent_rank"),
        F.cume_dist().over(w6).alias("val_percent_rank_cume_dist"),
        F.stddev_samp("val_percent_rank").over(w4).alias("plan_stddev_samp_val_percent_rank"),
    )
)

base = (
    scored
    .filter(F.col("val") >= 36)
    .select(
        "*",
        (F.col("plan_max_val_dense_rank_2_share") - F.col("plan_stddev_samp_val_percent_rank")).alias("plan_max_val_dense_rank_2_share_minus_plan_stddev_samp_val_percent_rank"),
        F.greatest("val_dense_rank_2_share_percent_rank", "plan_max_val_dense_rank_2_share").alias("greatest_val_dense_rank_2_share_percent_rank_plan_max_val_dense_rank_2_share"),
    )
)

result = (
    base
    .select(
        "id",
        "currency",
        "val_dense_rank_2_share",
        "plan_max_val_dense_rank_2_share",
        "val_dense_rank_2_share_percent_rank",
        "plan_stddev_samp_val_percent_rank",
        "greatest_val_dense_rank_2_share_percent_rank_plan_max_val_dense_rank_2_share",
        F.when(F.col("currency").rlike("^(THB|EUR)$"), "EXTENDED").when(F.col("currency").rlike("^(USD)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("currency_zone"),
    )
    .orderBy(
        F.col("greatest_val_dense_rank_2_share_percent_rank_plan_max_val_dense_rank_2_share").desc_nulls_last(),
    )
)
