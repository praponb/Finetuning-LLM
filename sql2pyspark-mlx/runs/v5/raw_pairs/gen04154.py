from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("plan")
w2 = Window.orderBy(F.col("val").desc_nulls_first())
w_ordered = Window.orderBy(F.col("plan_sum_val").asc_nulls_first())

lines = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode_outer("tier_limits").alias("val"))
)

filtered = (
    lines
    .filter(F.col("effective_from") > "2023-11-25")
    .select("*", F.col("val").cast("string").alias("val_num"))
)

enriched = (
    filtered
    .filter(F.col("val") == 78)
    .select(
        "id",
        "effective_from",
        "val",
        F.sum("val").over(w_running).alias("plan_sum_val"),
        F.max("val").over(w_running).alias("plan_max_val"),
        F.rank().over(w2).alias("val_rank"),
    )
)

with_windows = (
    enriched
    .select("*", F.dense_rank().over(w_ordered).alias("plan_sum_val_dense_rank"))
)

base = (
    with_windows
    .filter(F.col("plan_sum_val_dense_rank") != 6)
    .select(
        "*",
        F.greatest("plan_max_val", "plan_sum_val").alias("greatest_plan_max_val_plan_sum_val"),
    )
)

result = (
    base
    .select(
        "effective_from",
        "val",
        "plan_sum_val",
        "plan_max_val",
        "val_rank",
        "plan_sum_val_dense_rank",
        "greatest_plan_max_val_plan_sum_val",
    )
    .orderBy(F.col("effective_from").desc(), "plan_max_val")
)
