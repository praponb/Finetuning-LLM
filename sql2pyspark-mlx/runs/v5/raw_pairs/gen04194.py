from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("tier_limits").alias("val"))
)

result = (
    lines
    .groupBy("plan")
    .agg(
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.count(F.when(F.col("plan") == "starter", 1)).alias("starter_rows"),
        F.avg(F.when(F.col("plan") == "starter", F.col("val"))).alias("starter_avg_val"),
    )
    .orderBy(F.col("plan").desc())
)
