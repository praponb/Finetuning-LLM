from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.sum(F.when(F.col("category") == "Sports", 1).otherwise(0)).alias("sports_count"),
        F.count("stock_qty").alias("stock_qty_count"),
    )
)

result = (
    df_suppliers.alias("suppliers")
    .join(products_by_supplier.alias("t"), F.col("suppliers.id") == F.col("t.supplier_id"), "left")
    .select(
        F.col("suppliers.country"),
        F.col("t.sports_count"),
        F.col("t.stock_qty_count"),
    )
    .orderBy("country", "stock_qty_count")
)
