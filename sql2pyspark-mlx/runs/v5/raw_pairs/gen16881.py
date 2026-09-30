from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .filter(F.array_contains(F.col("p.tags"), "clearance") & F.col("ss.store_id").isin([1, 4, 6]))
    .select(
        F.col("ss.id"),
        F.col("ss.product_id"),
        F.col("p.category"),
        F.col("p.stock_qty"),
        F.col("s.region"),
    )
    .orderBy("id")
    .limit(5)
)
