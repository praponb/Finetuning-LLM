from pyspark.sql import functions as F

filtered = (
    df_suppliers
    .filter(F.length("supplier_name") > 12)
    .select(
        "*",
        F.coalesce("supplier_name", F.lit("N/A")).alias("supplier_name_filled"),
        F.coalesce("city", F.lit("N/A")).alias("city_clean"),
    )
)

cleaned = (
    filtered
    .filter(F.col("city").isNotNull() & F.col("country").isin(["US", "TH", "DE"]))
    .withColumn("supplier_name_city", F.concat("supplier_name", F.lit(" - "), "city"))
    .withColumn("rating_band", F.when(F.col("rating") >= 4.0, "High").when(F.col("rating") >= 1.7, "Medium").otherwise("Low"))
)

kept = (
    cleaned
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.price") >= 520), "left_anti")
)

result = (
    kept
    .groupBy("country")
    .pivot("city", ["Bangkok", "Berlin", "London"])
    .agg(F.max("rating"))
)
