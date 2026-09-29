from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("tier_value"),
    )
)

result = (
    lines
    .select(
        "id",
        "effective_from",
        "currency",
        "tier_value",
        F.when(F.col("currency").rlike("^(USD|THB)$"), "GROUP_B").otherwise("GROUP_A").alias("currency_zone"),
    )
    .orderBy(F.col("currency").asc_nulls_last())
)
