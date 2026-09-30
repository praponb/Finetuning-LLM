from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(F.round(F.stddev("stock_qty"), 2).alias("stddev_stock_qty"))
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_totals.alias("SupplierTotals"), F.col("suppliers.id") == F.col("SupplierTotals.supplier_id"), "inner")
    .filter(F.col("SupplierTotals.stddev_stock_qty") >= 26)
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.supplier_name"),
        F.col("suppliers.rating"),
        F.col("SupplierTotals.stddev_stock_qty"),
    )
)
