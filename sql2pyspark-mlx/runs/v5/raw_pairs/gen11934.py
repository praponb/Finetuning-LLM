from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .filter(F.col("cost").isNull())
    .groupBy("supplier_id")
    .agg(
        F.max("launch_date").alias("last_launch_date"),
        F.max("stock_qty").alias("max_stock_qty"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "left")
    .filter(F.col("ss.max_stock_qty") > 210)
    .select(
        F.col("s.id"),
        F.col("s.supplier_name"),
        F.col("ss.last_launch_date"),
        F.col("ss.max_stock_qty"),
    )
    .orderBy("id")
)
