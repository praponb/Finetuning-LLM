from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.sum("price").alias("total_price"),
        F.round(F.stddev("stock_qty"), 2).alias("stddev_stock_qty"),
    )
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_summary.alias("supplier_summary"), F.col("suppliers.id") == F.col("supplier_summary.supplier_id"), "left")
    .filter(F.col("supplier_summary.total_price") > 5700)
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.country"),
        F.col("suppliers.rating"),
        F.col("supplier_summary.total_price"),
        F.col("supplier_summary.stddev_stock_qty"),
    )
    .orderBy(F.col("id").desc(), "stddev_stock_qty")
)
