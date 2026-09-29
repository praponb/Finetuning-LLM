from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .groupBy("supplier_id")
    .agg(F.sum("stock_qty").alias("total_stock_qty"))
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_summary.alias("supplier_summary"), F.col("suppliers.id") == F.col("supplier_summary.supplier_id"), "inner")
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.city"),
        F.col("supplier_summary.total_stock_qty"),
    )
)
