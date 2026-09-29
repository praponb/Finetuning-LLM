from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("plan")
w2 = Window.partitionBy("currency", "plan").orderBy(F.col("val").desc())
w3 = Window.partitionBy("currency", "plan")
w_part = Window.orderBy(F.col("val_dense_rank").desc())
w5 = Window.partitionBy("currency")
w6 = Window.orderBy("plan_avg_val")

lines = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("discounts").alias("val"))
)

with_windows = (
    lines
    .select(
        "*",
        F.avg("val").over(w_recent).alias("plan_avg_val"),
        F.dense_rank().over(w2).alias("val_dense_rank"),
        F.count("val").over(w3).alias("currency_count_val"),
    )
)

enriched = (
    with_windows
    .withColumn("val_dense_rank_cume_dist", F.cume_dist().over(w_part))
    .withColumn("currency_min_currency_count_val", F.min("currency_count_val").over(w5))
    .withColumn("plan_avg_val_rank", F.rank().over(w6))
    .withColumn("plan_sum_currency_count_val", F.sum("currency_count_val").over(w_recent))
    .withColumn("val_dense_rank_share", F.round((F.col("val_dense_rank") / F.sum("val_dense_rank").over(w5)), 4))
)

result = (
    enriched
    .select(
        "id",
        "plan",
        "currency",
        "val",
        "val_dense_rank",
        "currency_min_currency_count_val",
        "plan_avg_val_rank",
        "plan_sum_currency_count_val",
        "val_dense_rank_share",
        F.when(F.col("plan").rlike("^(enterprise|basic)$"), "PRIORITY").otherwise("EXTENDED").alias("plan_zone"),
    )
)
