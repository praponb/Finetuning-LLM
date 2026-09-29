from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("tier_prices").alias("element"),
    )
)

result = (
    lines
    .select(
        "plan",
        F.when(F.col("plan").rlike("^(pro|basic)$"), "GROUP_A").when(F.col("plan").rlike("^(starter|enterprise)$"), "EXTENDED").otherwise("CORE").alias("plan_zone"),
    )
    .orderBy(F.col("plan_zone").desc_nulls_last(), "plan")
)
