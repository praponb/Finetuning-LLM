from pyspark.sql import functions as F

not_flagged = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.category") != "Home"), "left_semi")
)

result = (
    not_flagged
    .groupBy(
        F.when(F.col("rating") >= 4.4, "HIGH").otherwise("LOW").alias("rating_tier"),
    )
    .agg(
        F.min("rating").alias("min_rating"),
        F.percentile_approx("rating", [0.25, 0.5, 0.75]).alias("rating_quartiles"),
        F.count("*").alias("event_count"),
    )
)
