from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .filter(F.col("stock_qty") == 356)
    .groupBy("supplier_id")
    .agg(
        F.sum("stock_qty").alias("total_stock_qty"),
        F.concat_ws(", ", F.sort_array(F.collect_set("category"))).alias("category_values"),
    )
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_stats.alias("supplier_stats"), F.col("suppliers.id") == F.col("supplier_stats.supplier_id"), "inner")
    .filter(F.col("supplier_stats.total_stock_qty") >= 700)
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.supplier_name"),
        F.col("supplier_stats.total_stock_qty"),
        F.col("supplier_stats.category_values"),
    )
)
