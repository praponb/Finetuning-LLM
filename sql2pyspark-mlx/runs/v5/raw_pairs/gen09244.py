from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("tier_value").desc())
w2 = Window.partitionBy("plan").orderBy("is_enterprise")
w_part = Window.partitionBy("plan")
w4 = Window.partitionBy("plan").orderBy("tier_value")
w5 = Window.partitionBy("plan").orderBy(F.col("is_enterprise").desc_nulls_last())
w6 = Window.partitionBy("plan").orderBy(F.col("is_enterprise_rank").desc_nulls_last())
w7 = Window.partitionBy("plan").orderBy(F.col("tier_value_cume_dist_2").desc_nulls_first())
w8 = Window.partitionBy("plan").orderBy(F.col("tier_value_cume_dist_2").desc_nulls_last())

exploded = df_price_tiers.select("id", "plan", F.explode("tier_limits").alias("tier_value"))

filtered = (
    exploded
    .filter(F.col("plan") == "starter")
    .withColumn("is_enterprise", F.when(F.col("plan") == "enterprise", 1).otherwise(0))
)

metrics = (
    filtered
    .filter(F.col("tier_value").between(61, 65))
    .withColumn("tier_value_cume_dist", F.cume_dist().over(w))
    .withColumn("is_enterprise_rank", F.rank().over(w2))
    .withColumn("plan_max_tier_value", F.max("tier_value").over(w_part))
    .withColumn("plan_min_tier_value", F.min("tier_value").over(w_part))
    .withColumn("tier_value_cume_dist_2", F.cume_dist().over(w4))
)

enriched = (
    metrics
    .select(
        "*",
        F.stddev_samp("plan_min_tier_value").over(w_part).alias("plan_stddev_samp_plan_min_tier_value"),
    )
)

enriched_price_tiers = enriched.select("*", F.rank().over(w5).alias("is_enterprise_rank_2"))

scored = (
    enriched_price_tiers
    .select(
        "*",
        F.percent_rank().over(w6).alias("is_enterprise_rank_percent_rank"),
        F.rank().over(w7).alias("tier_value_cume_dist_2_rank"),
        F.rank().over(w8).alias("tier_value_cume_dist_2_rank_2"),
    )
)

kept_keys = (
    scored
    .select("plan", "id")
    .intersect(
        df_price_tiers
        .filter(F.col("plan") != "enterprise")
        .select("plan", "id")
    )
)

kept_rows = (
    scored
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .withColumn("plan_min_tier_value_tier", F.when(F.col("plan_min_tier_value") >= 68, "HIGH").otherwise("LOW"))
    .rollup("plan_min_tier_value_tier")
    .agg(
        F.count(F.when(F.col("plan") == "starter", 1)).alias("starter_count"),
        F.countDistinct("plan").alias("distinct_plans"),
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("distinct_plans").desc())
)
