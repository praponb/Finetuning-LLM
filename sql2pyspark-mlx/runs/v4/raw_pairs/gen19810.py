from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy(F.col("val").asc_nulls_last())
w3 = Window.orderBy(F.col("val").asc_nulls_first())
w4 = Window.partitionBy("plan").orderBy(F.col("val").desc_nulls_last())
w5 = Window.partitionBy("plan").orderBy("val")
w6 = Window.partitionBy("plan").orderBy(F.col("val_cume_dist").asc_nulls_first())
w7 = Window.orderBy(F.col("plan_avg_val").desc())

lines = df_price_tiers.select("id", "plan", F.explode_outer("tier_prices").alias("val"))

enriched = (
    lines
    .select(
        "*",
        F.avg("val").over(w).alias("plan_avg_val"),
        F.rank().over(w2).alias("val_rank"),
        F.cume_dist().over(w3).alias("val_cume_dist"),
        F.rank().over(w4).alias("val_rank_2"),
        F.dense_rank().over(w5).alias("val_dense_rank"),
    )
)

base = (
    enriched
    .filter(F.col("plan").isin(["enterprise", "starter", "pro"]))
    .select(
        "*",
        (F.col("val_rank") - F.col("val_dense_rank")).alias("val_rank_minus_val_dense_rank"),
    )
)

metrics = (
    base
    .withColumn("val_cume_dist_dense_rank", F.dense_rank().over(w6))
    .withColumn("plan_avg_val_rank", F.rank().over(w7))
)

result = (
    metrics
    .groupBy("plan")
    .agg(
        F.countDistinct("plan").alias("distinct_plans"),
        F.round(F.sum("val_cume_dist_dense_rank") / F.nullif(F.count(F.when(F.col("plan") == "pro", 1)), F.lit(0)), 2).alias("val_cume_dist_dense_rank_per_pro"),
    )
    .orderBy(F.col("plan").desc())
)
