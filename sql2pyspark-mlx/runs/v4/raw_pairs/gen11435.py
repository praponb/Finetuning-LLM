from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode("tier_limits").alias("tier_value"),
    )
)

result = (
    lines
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_value",
        F.when(F.col("plan").rlike("^(starter|enterprise)$"), "EXTENDED").when(F.col("plan").rlike("^(basic|pro)$"), "GROUP_A").otherwise("CORE").alias("plan_zone"),
    )
    .orderBy(F.col("tier_value").asc_nulls_last())
)
