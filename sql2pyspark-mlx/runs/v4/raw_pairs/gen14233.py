from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.array_contains(F.col("p.tags"), "bestseller") & F.col("ss.store_id").isin([1, 4]))
    .select(
        F.col("ss.id"),
        F.col("ss.revenue"),
        F.col("p.brand"),
        F.col("p.product_name"),
    )
    .orderBy(F.col("brand").desc())
)
