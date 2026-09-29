from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.col("product_name").isNotNull())
    .groupBy("supplier_id")
    .agg(
        F.sum(F.when(F.col("category") == "Books", 1).otherwise(0)).alias("books_count"),
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
    )
)

result = (
    df_suppliers.alias("suppliers")
    .join(products_by_supplier.alias("products_by_supplier"), F.col("suppliers.id") == F.col("products_by_supplier.supplier_id"), "inner")
    .select(
        F.col("suppliers.rating"),
        F.col("suppliers.city"),
        F.col("products_by_supplier.books_count"),
        F.col("products_by_supplier.stddev_cost"),
    )
    .orderBy("books_count", F.col("rating").desc())
)
