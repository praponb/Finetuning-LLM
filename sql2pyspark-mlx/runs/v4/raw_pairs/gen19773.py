from pyspark.sql import functions as F

metrics_long = (
    df_products
    .select("id", "supplier_id", "price", "cost")
    .unpivot(["id", "supplier_id"], ["price", "cost"], "measure", "reading")
    .filter(F.col("reading").isNotNull())
)

result = (
    metrics_long
    .cube(
        "measure",
        F.when(F.col("reading") >= 61, "LARGE").otherwise("SMALL").alias("reading_tier"),
    )
    .agg(
        F.percentile_approx("reading", 0.9).alias("reading_p90"),
        F.count(F.when(F.col("measure") == "price", 1)).alias("price_count"),
    )
    .orderBy(F.col("reading_p90").desc_nulls_first())
)
