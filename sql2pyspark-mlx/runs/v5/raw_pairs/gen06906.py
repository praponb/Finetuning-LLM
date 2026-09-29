from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("discounts").alias("element"))
)

base = (
    lines
    .filter(F.col("plan") == "pro")
    .withColumn("clean_currency", F.trim("currency"))
    .withColumn("currency_upper", F.upper("currency"))
)

base_price_tiers = (
    base
    .filter(F.col("element").isNotNull())
    .withColumn("currency_length", F.length("currency"))
)

valid = (
    base_price_tiers
    .filter(F.col("element") >= 52)
    .select(
        "id",
        "plan",
        "currency",
        "element",
        "clean_currency",
        F.when(F.col("currency").isin(["THB", "USD"]), "Tier 1").otherwise("Other").alias("currency_group"),
    )
)

result = (
    valid
    .select(
        "id",
        "plan",
        "currency",
        "element",
        "currency_group",
        F.when(F.col("currency").rlike("^(THB|EUR)$"), "GROUP_B").when(F.col("currency").rlike("^(USD)$"), "CORE").otherwise("PRIORITY").alias("currency_zone"),
    )
    .orderBy(F.col("element").desc())
)
