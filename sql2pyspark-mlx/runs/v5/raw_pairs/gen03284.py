from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.min("stock_qty").alias("min_stock_qty"),
        F.sum("stock_qty").alias("total_stock_qty"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "inner")
    .filter(F.col("pbs.total_stock_qty") >= 600)
    .select(
        F.col("s.id"),
        F.col("s.supplier_name"),
        F.col("s.city"),
        F.col("pbs.min_stock_qty"),
        F.col("pbs.total_stock_qty"),
    )
)
