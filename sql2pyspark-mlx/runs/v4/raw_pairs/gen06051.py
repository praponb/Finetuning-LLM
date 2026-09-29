from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "plan", F.explode_outer("tier_limits").alias("tier_value"))
)

result = (
    exploded
    .select(
        "plan",
        "tier_value",
        F.when(F.col("plan").rlike("^(pro|enterprise)$"), "PRIORITY").when(F.col("plan").rlike("^(starter|basic)$"), "GROUP_B").otherwise("GROUP_A").alias("plan_zone"),
    )
    .orderBy("plan_zone")
)
