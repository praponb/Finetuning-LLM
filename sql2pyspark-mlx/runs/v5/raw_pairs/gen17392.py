from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("discounts").alias("tier_value"),
    )
)

result = (
    lines
    .select(
        "id",
        "plan",
        "effective_from",
        F.when(F.col("plan").rlike("^(starter|pro)$"), "OTHER_ZONE").when(F.col("plan").rlike("^(basic|enterprise)$"), "EXTENDED").otherwise("GROUP_B").alias("plan_zone"),
    )
    .orderBy(F.col("plan_zone").asc_nulls_first())
)
