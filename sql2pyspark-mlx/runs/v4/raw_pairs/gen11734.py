from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .filter(F.col("stock_qty").between(111, 340))
    .groupBy("supplier_id")
    .agg(
        F.max("stock_qty").alias("max_stock_qty"),
        F.max("launch_date").alias("latest_launch_date"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "left")
    .select(
        F.col("s.supplier_name"),
        F.col("s.country"),
        F.col("t.max_stock_qty"),
        F.col("t.latest_launch_date"),
    )
    .orderBy("country")
)
