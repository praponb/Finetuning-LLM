from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_first())
w2 = Window.orderBy(F.col("tier_value").asc_nulls_first())
w3 = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_last())
w4 = Window.partitionBy("plan").orderBy(F.col("tier_value_rank").desc())
w5 = Window.partitionBy("plan")
w6 = Window.partitionBy("plan").orderBy(F.col("tier_value_rank_2").asc_nulls_last())
w7 = Window.partitionBy("currency", "plan").orderBy(F.col("tier_value").desc_nulls_first())
w8 = Window.orderBy(F.col("tier_value_percent_rank").asc_nulls_last())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("tier_value"),
    )
)

enriched = (
    lines
    .withColumn("tier_value_rank", F.rank().over(w_grp))
    .withColumn("tier_value_rank_2", F.rank().over(w2))
    .withColumn("tier_value_percent_rank", F.percent_rank().over(w3))
)

filtered = (
    enriched
    .filter(F.col("effective_from") >= F.lit("2024-03-23").cast("date"))
    .withColumn("tier_value_percent_rank_level", F.when(F.col("tier_value_percent_rank") >= 0.86, "High").when(F.col("tier_value_percent_rank") >= 0.39, "Medium").otherwise("Low"))
)

enriched_price_tiers = (
    filtered
    .select(
        "plan",
        "tier_value",
        "tier_value_percent_rank",
        "tier_value_percent_rank_level",
        F.cume_dist().over(w4).alias("tier_value_rank_cume_dist"),
        F.min("tier_value_percent_rank").over(w5).alias("plan_min_tier_value_percent_rank"),
        F.cume_dist().over(w6).alias("tier_value_rank_2_cume_dist"),
        F.percent_rank().over(w7).alias("tier_value_percent_rank_2"),
        F.cume_dist().over(w8).alias("tier_value_percent_rank_cume_dist"),
    )
)

result = (
    enriched_price_tiers
    .groupBy(
        F.when(F.col("tier_value") >= 85, "PREMIUM").otherwise("STANDARD").alias("tier_value_tier"),
    )
    .agg(
        F.min("tier_value_percent_rank").alias("min_tier_value_percent_rank"),
        F.percentile_approx("tier_value_rank_cume_dist", 0.95).alias("tier_value_rank_cume_dist_p95"),
        F.round(F.avg("tier_value_rank_2_cume_dist"), 2).alias("avg_tier_value_rank_2_cume_dist"),
        F.percentile_approx("tier_value_rank_cume_dist", [0.25, 0.5, 0.75]).alias("tier_value_rank_cume_dist_quartiles"),
        F.size(F.collect_set("plan")).alias("num_plans"),
    )
    .filter(F.col("num_plans") < 1000)
    .orderBy("tier_value_tier")
)
