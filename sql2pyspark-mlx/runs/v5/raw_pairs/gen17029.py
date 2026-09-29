from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("discounts").alias("tier_value"),
    )
)

result = (
    exploded
    .select(
        "id",
        "plan",
        "effective_from",
        F.when(F.col("plan").rlike("^(enterprise|starter)$"), "CORE").otherwise("PRIORITY").alias("plan_zone"),
    )
    .orderBy(F.col("id").desc_nulls_last())
)
