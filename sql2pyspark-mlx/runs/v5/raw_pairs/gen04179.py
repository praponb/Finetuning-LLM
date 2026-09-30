from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("val").desc())
w_hist = Window.partitionBy("plan")
w3 = Window.partitionBy("plan").orderBy(F.col("plan_avg_val").desc())
w4 = Window.orderBy(F.col("val_dense_rank").desc_nulls_last())
w5 = Window.orderBy(F.col("plan_avg_val").desc_nulls_last())
w6 = Window.orderBy(F.col("plan_max_val").asc_nulls_last())
w7 = Window.orderBy(F.col("val_dense_rank").asc_nulls_last())
w8 = Window.orderBy("plan_max_val_rank")

flattened = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode_outer("discounts").alias("val"))
)

scored = (
    flattened
    .select(
        "*",
        F.dense_rank().over(w).alias("val_dense_rank"),
        F.avg("val").over(w_hist).alias("plan_avg_val"),
    )
)

enriched = (
    scored
    .filter(F.col("effective_from") <= "2023-09-01")
    .select(
        "val_dense_rank",
        "plan_avg_val",
        F.percent_rank().over(w3).alias("plan_avg_val_percent_rank"),
        F.max("val").over(w_hist).alias("plan_max_val"),
        F.cume_dist().over(w4).alias("val_dense_rank_cume_dist"),
    )
)

enriched_price_tiers = (
    enriched
    .select(
        "plan_avg_val",
        "val_dense_rank_cume_dist",
        F.rank().over(w5).alias("plan_avg_val_rank"),
        F.rank().over(w6).alias("plan_max_val_rank"),
        F.percent_rank().over(w7).alias("val_dense_rank_percent_rank"),
    )
)

base = (
    enriched_price_tiers
    .filter(F.col("plan_avg_val_rank") >= 7)
    .withColumn("adjusted_plan_max_val_rank", F.round((F.col("plan_max_val_rank") * 1.1), 2))
)

result = (
    base
    .select(
        "plan_avg_val_rank",
        "plan_max_val_rank",
        "adjusted_plan_max_val_rank",
        F.dense_rank().over(w8).alias("plan_max_val_rank_dense_rank"),
    )
)
