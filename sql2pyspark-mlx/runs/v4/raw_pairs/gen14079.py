from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.orderBy(F.col("val").desc())
w3 = Window.orderBy(F.col("val").desc_nulls_last())
w4 = Window.partitionBy("plan").orderBy(F.col("plan_max_val").desc())
w5 = Window.partitionBy("plan").orderBy(F.col("plan_sum_val").desc())
w6 = Window.orderBy(F.col("plan_max_val_dense_rank").desc_nulls_last())
w7 = Window.partitionBy("plan").orderBy(F.col("plan_max_val").desc_nulls_last())

lines = df_price_tiers.select("id", "plan", F.explode_outer("tier_limits").alias("val"))

with_windows = (
    lines
    .select(
        "*",
        F.count("val").over(w).alias("plan_count_val"),
        F.max("val").over(w).alias("plan_max_val"),
        F.sum("val").over(w).alias("plan_sum_val"),
        F.cume_dist().over(w2).alias("val_cume_dist"),
        F.cume_dist().over(w3).alias("val_cume_dist_2"),
    )
)

remaining_keys = (
    with_windows
    .select("id", "plan")
    .exceptAll(
        df_price_tiers
        .filter(F.col("currency").isin(["EUR", "USD", "THB"]))
        .select("id", "plan")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

with_windows_price_tiers = (
    kept_rows
    .select(
        "*",
        F.dense_rank().over(w4).alias("plan_max_val_dense_rank"),
        F.cume_dist().over(w5).alias("plan_sum_val_cume_dist"),
        F.round((F.col("plan_count_val") / F.sum("plan_count_val").over(w)), 4).alias("plan_count_val_share"),
    )
)

matched_keys = (
    with_windows_price_tiers
    .select("id", "plan")
    .subtract(
        df_price_tiers
        .filter(F.col("currency").isin(["EUR", "THB", "USD"]))
        .select("id", "plan")
    )
)

kept_rows_price_tiers = (
    with_windows_price_tiers
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

with_windows_price_tiers_2 = (
    kept_rows_price_tiers
    .withColumn("plan_max_val_dense_rank_dense_rank", F.dense_rank().over(w6))
    .withColumn("plan_max_val_rank", F.rank().over(w7))
    .withColumn("plan_stddev_samp_plan_sum_val", F.stddev_samp("plan_sum_val").over(w))
    .withColumn("plan_sum_plan_max_val", F.sum("plan_max_val").over(w))
)

result = (
    with_windows_price_tiers_2
    .select(
        "id",
        "plan",
        "val",
        "plan_count_val",
        "plan_max_val",
        "plan_sum_val",
        "val_cume_dist",
        "val_cume_dist_2",
        "plan_sum_val_cume_dist",
    )
    .orderBy(F.col("plan_sum_val_cume_dist").desc_nulls_last())
)
