from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("val").desc_nulls_last())
w2 = Window.partitionBy("plan")
w3 = Window.partitionBy("currency", "plan").orderBy(F.col("val").asc_nulls_first())
w4 = Window.partitionBy("plan").orderBy(F.col("val_rank_2_rounded").desc())
w5 = Window.partitionBy("currency")

lines = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("tier_limits").alias("val"))
)

scored = (
    lines
    .withColumn("val_rank", F.rank().over(w))
    .withColumn("val_rank_2", F.rank().over(w))
)

valid = (
    scored
    .filter(F.col("val") < 24)
    .select("*", F.round("val_rank_2", 2).alias("val_rank_2_rounded"))
)

metrics = (
    valid
    .select(
        "*",
        F.min("val").over(w2).alias("plan_min_val"),
        F.percent_rank().over(w3).alias("val_percent_rank"),
        F.dense_rank().over(w4).alias("val_rank_2_rounded_dense_rank"),
    )
)

scored_price_tiers = (
    metrics
    .withColumn("val_rank_2_rounded_dense_rank_share", F.round((F.col("val_rank_2_rounded_dense_rank") / F.sum("val_rank_2_rounded_dense_rank").over(w5)), 4))
    .withColumn("currency_min_val_percent_rank", F.min("val_percent_rank").over(w5))
)

cleaned = (
    scored_price_tiers
    .filter(F.col("currency_min_val_percent_rank") > 1)
    .withColumn("currency_min_val_percent_rank_num", F.col("currency_min_val_percent_rank").cast("double"))
)

result = (
    cleaned
    .select(
        "id",
        "val",
        "val_rank_2_rounded",
        "plan_min_val",
        "val_percent_rank",
        "currency_min_val_percent_rank",
        "currency_min_val_percent_rank_num",
    )
    .orderBy(F.col("val_percent_rank").desc_nulls_last())
)
