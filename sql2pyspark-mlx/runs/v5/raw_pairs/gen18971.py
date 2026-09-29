from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.sum("stock_qty").alias("total_stock_qty"),
        F.min("cost").alias("min_cost"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "inner")
    .select(
        F.col("s.country"),
        F.col("s.city"),
        F.col("pbs.total_stock_qty"),
        F.col("pbs.min_cost"),
    )
    .orderBy(F.col("min_cost").desc(), "city")
)
