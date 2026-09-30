from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(F.round(F.stddev("stock_qty"), 2).alias("stddev_stock_qty"))
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_totals.alias("supplier_totals"), F.col("suppliers.id") == F.col("supplier_totals.supplier_id"), "inner")
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.country"),
        F.col("supplier_totals.stddev_stock_qty"),
    )
    .orderBy(F.col("id").desc(), F.col("stddev_stock_qty").desc())
)
