from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("tier_value"),
    )
)

result = (
    lines
    .select(
        "id",
        "plan",
        "tier_value",
        F.when(F.col("plan").rlike("^(starter|pro)$"), "PRIORITY").when(F.col("plan").rlike("^(basic|enterprise)$"), "EXTENDED").otherwise("CORE").alias("plan_zone"),
    )
)
