from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.min("launch_date").alias("first_launch_date"),
        F.sum("stock_qty").alias("total_stock_qty"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "inner")
    .filter(F.col("pbs.total_stock_qty") > 700)
    .select(
        F.col("s.id"),
        F.col("s.supplier_name"),
        F.col("pbs.first_launch_date"),
        F.col("pbs.total_stock_qty"),
    )
)
