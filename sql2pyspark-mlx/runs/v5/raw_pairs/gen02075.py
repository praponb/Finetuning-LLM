from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("discounts").alias("element"))
)

result = (
    lines
    .groupBy(
        F.when(F.col("element") >= 70, "HIGH").otherwise("LOW").alias("element_tier"),
    )
    .agg(
        F.percentile_approx("element", [0.25, 0.5, 0.75]).alias("element_quartiles"),
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_rows"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("eur_rows").desc())
)
