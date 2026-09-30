from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.orderBy(F.col("val").desc_nulls_first())
w3 = Window.orderBy("val")

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("discounts").alias("val"),
    )
)

enriched = lines.withColumn("plan_sum_val", F.sum("val").over(w))

base = (
    enriched
    .filter((F.col("effective_from") >= F.lit("2024-03-13").cast("date")) & (F.col("plan_sum_val") <= 320))
    .select(
        "id",
        "val",
        "plan_sum_val",
        (F.col("val") - F.col("plan_sum_val")).alias("val_minus_plan_sum_val"),
        (F.col("val") - F.col("plan_sum_val")).alias("val_minus_plan_sum_val_2"),
    )
)

with_windows = (
    base
    .select(
        "val_minus_plan_sum_val",
        "val_minus_plan_sum_val_2",
        F.dense_rank().over(w2).alias("val_dense_rank"),
        F.rank().over(w3).alias("val_rank"),
    )
)

filtered = (
    with_windows
    .filter(F.col("val_minus_plan_sum_val_2").isNotNull())
    .withColumn("val_dense_rank_filled", F.coalesce("val_dense_rank", F.lit(0)))
    .withColumn("val_rank_plus_val_dense_rank", (F.col("val_rank") + F.col("val_dense_rank")))
)

result = (
    filtered
    .select(
        "val_minus_plan_sum_val",
        "val_minus_plan_sum_val_2",
        "val_dense_rank",
        "val_rank",
        "val_dense_rank_filled",
    )
    .orderBy(F.col("val_dense_rank_filled").desc_nulls_last())
)
