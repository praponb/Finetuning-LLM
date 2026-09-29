from pyspark.sql import functions as F

not_flagged = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.product_name").isNull()), "left_anti")
)

result = (
    not_flagged
    .groupBy(
        "city",
        F.when(F.col("rating") >= 4.6, "HIGH").otherwise("LOW").alias("rating_tier"),
    )
    .agg(
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_rows"),
        F.sum(F.when(F.col("city") == "Singapore", F.col("rating")).otherwise(0)).alias("singapore_rating"),
        F.stddev_samp("rating").alias("rating_stddev"),
        F.sum(F.when(F.col("country") == "UK", F.col("rating")).otherwise(0)).alias("uk_rating"),
    )
)
