from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("tier_value"),
    )
)

result = (
    flattened
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.when(F.col("plan").rlike("^(enterprise|starter)$"), "GROUP_B").when(F.col("plan").rlike("^(basic|pro)$"), "GROUP_A").otherwise("EXTENDED").alias("plan_zone"),
    )
    .orderBy(F.col("currency").asc_nulls_last())
)
