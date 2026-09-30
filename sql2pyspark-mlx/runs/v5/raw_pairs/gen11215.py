from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("tier_limits").alias("tier_value"),
    )
)

result = (
    flattened
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_value",
        F.when(F.col("plan").rlike("^(starter|enterprise)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("plan_zone"),
    )
    .orderBy(F.col("plan_zone").asc_nulls_first())
)
