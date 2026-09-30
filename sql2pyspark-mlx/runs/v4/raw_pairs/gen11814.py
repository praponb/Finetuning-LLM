from pyspark.sql import functions as F

result = (
    df_suppliers.alias("suppliers")
    .join(df_products.alias("products"), F.col("products.supplier_id") == F.col("suppliers.id"), "left")
    .filter(F.col("suppliers.country") == "SG")
    .groupBy(F.col("suppliers.country"))
    .agg(
        F.count(F.col("products.id")).alias("product_count"),
        F.sum(F.when(F.col("products.category") == "Books", F.col("products.stock_qty")).otherwise(0)).alias("books_stock_qty"),
    )
    .orderBy(F.col("books_stock_qty").desc(), "country")
    .limit(5)
)
