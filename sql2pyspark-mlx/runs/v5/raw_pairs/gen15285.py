from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.orderBy("val")
w3 = Window.partitionBy("plan").orderBy(F.col("val_percent_rank").desc())
w4 = Window.partitionBy("plan").orderBy(F.col("plan_sum_plan_sum_val").desc())
w_running = Window.partitionBy("plan").orderBy("val_percent_rank")

exploded = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("tier_limits").alias("val"))
)

filtered = (
    exploded
    .filter(F.col("val") > 61)
    .withColumn("plan_lower", F.lower("plan"))
    .withColumn("effective_from_month_end", F.last_day("effective_from"))
)

metrics = (
    filtered
    .select(
        "id",
        "plan",
        "effective_from",
        "plan_lower",
        F.count("val").over(w).alias("plan_count_val"),
        F.percent_rank().over(w2).alias("val_percent_rank"),
        F.min("val").over(w).alias("plan_min_val"),
        F.sum("val").over(w).alias("plan_sum_val"),
    )
)

metrics_price_tiers = (
    metrics
    .select(
        "plan",
        "val_percent_rank",
        F.sum("plan_sum_val").over(w).alias("plan_sum_plan_sum_val"),
    )
)

enriched = (
    metrics_price_tiers
    .select(
        "plan",
        "val_percent_rank",
        F.rank().over(w3).alias("val_percent_rank_rank"),
        F.cume_dist().over(w4).alias("plan_sum_plan_sum_val_cume_dist"),
        F.dense_rank().over(w_running).alias("val_percent_rank_dense_rank"),
    )
)

result = (
    enriched
    .select("plan", "val_percent_rank", "val_percent_rank_rank")
    .orderBy(F.col("val_percent_rank_rank").desc())
)
