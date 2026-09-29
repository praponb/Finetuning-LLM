from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.min("launch_date").alias("first_launch_date"),
        F.round(F.stddev("stock_qty"), 2).alias("stddev_stock_qty"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("ss.first_launch_date"),
        F.col("ss.stddev_stock_qty"),
    )
    .orderBy("stddev_stock_qty")
)
