from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("val"),
    )
)

result = (
    lines
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.when(F.col("plan").rlike("^(pro|enterprise)$"), "EXTENDED").otherwise("PRIORITY").alias("plan_zone"),
    )
    .orderBy(F.col("plan_zone").desc())
)
