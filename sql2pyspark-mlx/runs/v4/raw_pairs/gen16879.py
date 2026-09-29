from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy("val")
w_grp = Window.partitionBy("plan").orderBy(F.col("val").desc())
w_part_2 = Window.orderBy("val_dense_rank")
w5 = Window.orderBy(F.col("plan_stddev_samp_val").desc_nulls_last())
w6 = Window.orderBy("val_rank")
w7 = Window.orderBy(F.col("val_dense_rank_percent_rank").desc_nulls_first())

lines = df_price_tiers.select("id", "plan", F.explode_outer("discounts").alias("val"))

scored = (
    lines
    .withColumn("plan_max_val", F.max("val").over(w_ordered))
    .withColumn("val_dense_rank", F.dense_rank().over(w2))
    .withColumn("plan_stddev_samp_val", F.stddev_samp("val").over(w_ordered))
)

with_windows = (
    scored
    .select(
        "*",
        F.stddev_samp("plan_stddev_samp_val").over(w_ordered).alias("plan_stddev_samp_plan_stddev_samp_val"),
        F.min("plan_stddev_samp_val").over(w_ordered).alias("plan_min_plan_stddev_samp_val"),
        F.rank().over(w_grp).alias("val_rank"),
        F.percent_rank().over(w_part_2).alias("val_dense_rank_percent_rank"),
        F.rank().over(w5).alias("plan_stddev_samp_val_rank"),
    )
)

valid = (
    with_windows
    .filter(F.col("plan_stddev_samp_val_rank") > 4)
    .select(
        "val",
        "plan_max_val",
        "val_dense_rank",
        "plan_stddev_samp_val",
        "plan_min_plan_stddev_samp_val",
        "val_rank",
        "val_dense_rank_percent_rank",
        F.round("plan_stddev_samp_val_rank", 1).alias("plan_stddev_samp_val_rank_rounded"),
    )
)

enriched = (
    valid
    .select(
        "*",
        F.cume_dist().over(w6).alias("val_rank_cume_dist"),
        F.rank().over(w7).alias("val_dense_rank_percent_rank_rank"),
    )
)

result = (
    enriched
    .select(
        "val",
        "plan_max_val",
        "val_dense_rank",
        "plan_min_plan_stddev_samp_val",
        "val_rank",
        "val_dense_rank_percent_rank",
        "val_rank_cume_dist",
        "val_dense_rank_percent_rank_rank",
    )
)
