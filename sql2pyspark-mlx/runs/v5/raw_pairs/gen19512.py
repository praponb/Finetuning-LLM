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
        "id",
        "plan",
        "effective_from",
        "currency",
        "element",
        F.when(F.col("currency").rlike("^(USD|THB)$"), "EXTENDED").when(F.col("currency").rlike("^(EUR)$"), "OTHER_ZONE").otherwise("CORE").alias("currency_zone"),
    )
    .orderBy("id")
)
