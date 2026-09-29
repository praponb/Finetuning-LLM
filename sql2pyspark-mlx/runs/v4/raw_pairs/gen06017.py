from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("tier_limits").alias("element"))
)

base = (
    exploded
    .filter(F.col("element") >= 64)
    .select("*", F.trim("plan").alias("clean_plan"))
)

base_price_tiers = (
    base
    .filter(F.col("plan").isin(["basic", "starter", "pro"]))
    .select(
        "id",
        "plan",
        "currency",
        "element",
        "clean_plan",
        F.when(F.col("currency").isin(["EUR", "USD"]), "Core").otherwise("Other").alias("currency_group"),
        F.when(F.col("element") >= 80, "High").when(F.col("element") >= 26, "Medium").otherwise("Low").alias("element_tier"),
    )
)

result = (
    base_price_tiers
    .select(
        "id",
        "plan",
        "currency",
        "clean_plan",
        "currency_group",
        F.when(F.col("currency").rlike("^(USD|EUR)$"), "PRIORITY").when(F.col("currency").rlike("^(THB)$"), "GROUP_A").otherwise("GROUP_B").alias("currency_zone"),
    )
)
