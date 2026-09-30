from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy(F.col("effective_from_quarter").asc_nulls_first())
w3 = Window.partitionBy("plan").orderBy(F.col("tier_value").desc())
w_hist = Window.partitionBy("plan").orderBy(F.col("effective_from_quarter_percent_rank").desc_nulls_last())
w5 = Window.orderBy(F.col("effective_from_quarter_percent_rank").desc())

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode("tier_prices").alias("tier_value"),
    )
)

filtered = (
    exploded
    .filter(F.col("tier_value") <= 52)
    .select("*", F.quarter("effective_from").alias("effective_from_quarter"))
)

metrics = (
    filtered
    .filter(F.col("effective_from_quarter") >= 61)
    .select(
        "id",
        "plan",
        F.count("effective_from_quarter").over(w).alias("plan_count_effective_from_quarter"),
        F.percent_rank().over(w2).alias("effective_from_quarter_percent_rank"),
        F.dense_rank().over(w3).alias("tier_value_dense_rank"),
    )
)

metrics_price_tiers = (
    metrics
    .withColumn("effective_from_quarter_percent_rank_dense_rank", F.dense_rank().over(w_hist))
    .withColumn("effective_from_quarter_percent_rank_percent_rank", F.percent_rank().over(w5))
)

with_windows = (
    metrics_price_tiers
    .withColumn("plan_min_tier_value_dense_rank", F.min("tier_value_dense_rank").over(w))
    .withColumn("effective_from_quarter_percent_rank_cume_dist", F.cume_dist().over(w_hist))
)

result = (
    with_windows
    .groupBy("plan")
    .agg(
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_rows"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_rows"),
    )
)
