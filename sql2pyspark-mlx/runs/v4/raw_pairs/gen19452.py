from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("element"),
    )
)

result = (
    flattened
    .select(
        "plan",
        "effective_from",
        "element",
        F.when(F.col("plan").rlike("^(basic|pro)$"), "EXTENDED").when(F.col("plan").rlike("^(starter|enterprise)$"), "CORE").otherwise("GROUP_A").alias("plan_zone"),
    )
    .orderBy("element")
)
