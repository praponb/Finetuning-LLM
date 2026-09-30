from pyspark.sql import functions as F

lines = df_price_tiers.select("id", F.explode_outer("discounts").alias("element"))

result = (
    lines
    .groupBy(
        F.when(F.col("element") >= 82, "HIGH").otherwise("LOW").alias("element_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.percentile_approx("element", [0.25, 0.5, 0.75]).alias("element_quartiles"),
        F.percentile_approx("element", 0.95).alias("element_p95"),
    )
    .orderBy(F.col("cnt").desc())
)
