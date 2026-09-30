from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("tier_prices").alias("element"))
)

result = (
    exploded
    .select(
        "plan",
        "element",
        F.when(F.col("plan").rlike("^(pro|enterprise)$"), "PRIORITY").otherwise("GROUP_B").alias("plan_zone"),
    )
)
