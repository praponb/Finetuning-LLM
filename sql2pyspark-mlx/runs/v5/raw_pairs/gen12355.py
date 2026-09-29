from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy(F.col("val").desc_nulls_last())
w3 = Window.orderBy(F.col("val_dense_rank").desc())
w4 = Window.orderBy(F.col("plan_max_val").asc_nulls_first())
w5 = Window.partitionBy("plan").orderBy(F.col("val").desc())

lines = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode_outer("discounts").alias("val"))
)

enriched = (
    lines
    .filter(F.col("plan").isin(["pro", "starter"]))
    .select(
        "*",
        F.max("val").over(w).alias("plan_max_val"),
        F.dense_rank().over(w2).alias("val_dense_rank"),
    )
)

with_windows = (
    enriched
    .withColumn("val_dense_rank_percent_rank", F.percent_rank().over(w3))
    .withColumn("val_dense_rank_rank", F.rank().over(w3))
    .withColumn("plan_max_val_rank", F.rank().over(w4))
    .withColumn("val_rank", F.rank().over(w5))
    .withColumn("plan_stddev_samp_plan_max_val", F.stddev_samp("plan_max_val").over(w))
)

scored = (
    with_windows
    .withColumn("val_dense_rank_rank_share", F.round((F.col("val_dense_rank_rank") / F.sum("val_dense_rank_rank").over(w)), 4))
)

base = (
    scored
    .filter(F.col("plan").isin(["starter", "pro"]))
    .select(
        "id",
        "plan",
        "effective_from",
        "val",
        "plan_max_val",
        "val_dense_rank_percent_rank",
        "val_dense_rank_rank",
        "plan_max_val_rank",
        "val_rank",
        "plan_stddev_samp_plan_max_val",
        "val_dense_rank_rank_share",
        F.round("plan_stddev_samp_plan_max_val", 2).alias("plan_stddev_samp_plan_max_val_rounded"),
    )
)

result = (
    base
    .select(
        "id",
        "plan",
        "val",
        "plan_max_val",
        "val_dense_rank_percent_rank",
        "val_dense_rank_rank",
        "plan_max_val_rank",
        "val_rank",
        "plan_stddev_samp_plan_max_val",
        F.when(F.col("plan").rlike("^(starter|pro)$"), "OTHER_ZONE").when(F.col("plan").rlike("^(basic|enterprise)$"), "GROUP_B").otherwise("CORE").alias("plan_zone"),
    )
    .orderBy(F.col("val_dense_rank_percent_rank").desc_nulls_first())
)
