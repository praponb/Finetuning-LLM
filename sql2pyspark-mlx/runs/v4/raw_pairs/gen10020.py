from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "currency")
w2 = Window.partitionBy("plan", "currency").orderBy(F.col("val").asc_nulls_last())
w3 = Window.partitionBy("currency")
w4 = Window.partitionBy("plan").orderBy("val_dense_rank")
w5 = Window.partitionBy("currency", "plan")
w_grp = Window.partitionBy("plan")
w7 = Window.orderBy("val_dense_rank")
w8 = Window.orderBy(F.col("plan_avg_currency_sum_val").desc_nulls_last())

lines = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("tier_limits").alias("val"))
)

matched_keys = (
    lines
    .select("id", "currency")
    .exceptAll(
        df_price_tiers
        .filter(F.col("plan") == "pro")
        .select("id", "currency")
    )
)

reconciled = (
    lines
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.currency") == F.col("k.currency")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .select(
        "*",
        F.sum("val").over(w).alias("plan_sum_val"),
        F.dense_rank().over(w2).alias("val_dense_rank"),
        F.sum("val").over(w3).alias("currency_sum_val"),
    )
)

valid = (
    metrics
    .filter(F.col("currency") == "USD")
    .select(
        "id",
        "plan",
        "currency",
        "plan_sum_val",
        "val_dense_rank",
        "currency_sum_val",
        F.initcap(F.trim("plan")).alias("plan_title"),
        F.when(F.col("val_dense_rank") >= 7, "High").when(F.col("val_dense_rank") >= 3, "Medium").otherwise("Low").alias("val_dense_rank_tier"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.dense_rank().over(w4).alias("val_dense_rank_dense_rank"),
        F.count("plan_sum_val").over(w5).alias("currency_count_plan_sum_val"),
        F.avg("currency_sum_val").over(w_grp).alias("plan_avg_currency_sum_val"),
        F.min("currency_sum_val").over(w_grp).alias("plan_min_currency_sum_val"),
    )
)

with_windows = (
    scored
    .select(
        "*",
        F.round((F.col("val_dense_rank_dense_rank") / F.sum("val_dense_rank_dense_rank").over(w3)), 4).alias("val_dense_rank_dense_rank_share"),
        F.count("plan_avg_currency_sum_val").over(w_grp).alias("plan_count_plan_avg_currency_sum_val"),
        F.dense_rank().over(w7).alias("val_dense_rank_dense_rank_2"),
        F.dense_rank().over(w8).alias("plan_avg_currency_sum_val_dense_rank"),
        F.stddev_samp("plan_min_currency_sum_val").over(w_grp).alias("plan_stddev_samp_plan_min_currency_sum_val"),
    )
)

result = (
    with_windows
    .withColumn("currency_stddev_samp_plan_stddev_samp_plan_min_currency_sum_val", F.stddev_samp("plan_stddev_samp_plan_min_currency_sum_val").over(w3))
    .withColumn("currency_sum_plan_count_plan_avg_currency_sum_val", F.sum("plan_count_plan_avg_currency_sum_val").over(w3))
    .withColumn("val_dense_rank_dense_rank_share_2", F.round((F.col("val_dense_rank_dense_rank") / F.sum("val_dense_rank_dense_rank").over(w3)), 4))
)
