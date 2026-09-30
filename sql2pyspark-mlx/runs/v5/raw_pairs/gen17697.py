from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .filter(F.col("brand").isin(["Umbrella", "Globex", "Acme"]))
    .groupBy("supplier_id")
    .agg(
        F.sum("stock_qty").alias("total_stock_qty"),
        F.sum("cost").alias("total_cost"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.supplier_name"),
        F.col("s.rating"),
        F.col("ss.total_stock_qty"),
        F.col("ss.total_cost"),
    )
    .orderBy(F.col("total_stock_qty").desc(), "id")
    .limit(50)
)
