from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("element"),
    )
)

result = (
    flattened
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.when(F.col("plan").rlike("^(pro|starter)$"), "EXTENDED").when(F.col("plan").rlike("^(basic|enterprise)$"), "CORE").otherwise("PRIORITY").alias("plan_zone"),
    )
    .orderBy(F.col("plan_zone").desc_nulls_first(), F.col("id").desc())
)
