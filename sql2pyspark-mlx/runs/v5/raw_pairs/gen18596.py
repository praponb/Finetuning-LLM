from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("discounts").alias("element"),
    )
)

result = (
    lines
    .groupBy(
        F.when(F.col("element") >= 70, "PREMIUM").otherwise("STANDARD").alias("element_tier"),
    )
    .agg(
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_count"),
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_rows"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_rows"),
        F.max(F.when(F.col("plan") == "pro", F.col("element"))).alias("pro_max_element"),
    )
    .orderBy(F.col("basic_rows").desc_nulls_last())
)
