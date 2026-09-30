from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("ss.product_id") == F.col("p.id"), "left")
    .filter(F.array_contains(F.col("p.tags"), "premium") & F.col("ss.product_id").isin([3, 10]))
    .select(
        F.col("p.category"),
        F.col("p.cost"),
        F.col("ss.units"),
        F.col("ss.revenue"),
        F.col("ss.store_id"),
    )
)
