from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .filter(F.col("category") == "Electronics")
    .groupBy("supplier_id")
    .agg(
        F.max("price").alias("max_price"),
        F.max("stock_qty").alias("max_stock_qty"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "left")
    .filter(F.col("ss.max_price") >= 540)
    .select(F.col("s.rating"), F.col("ss.max_price"), F.col("ss.max_stock_qty"))
)
