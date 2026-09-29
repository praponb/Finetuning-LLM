from pyspark.sql import functions as F

exploded = df_price_tiers.select("id", "plan", F.explode("tier_prices").alias("element"))

result = (
    exploded
    .groupBy("plan")
    .agg(
        F.countDistinct("plan").alias("distinct_plans"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.size(F.collect_set("plan")).alias("num_plans"),
    )
    .orderBy(F.col("distinct_plans").desc_nulls_last())
)
