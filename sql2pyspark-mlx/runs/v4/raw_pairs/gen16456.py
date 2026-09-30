from pyspark.sql import functions as F

result = (
    df_suppliers.alias("suppliers")
    .join(df_products.alias("products"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .groupBy(F.col("suppliers.city"))
    .agg(
        F.count(F.col("products.id")).alias("product_count"),
        F.sum(F.when(F.col("products.category") == "Fashion", F.col("products.stock_qty")).otherwise(0)).alias("fashion_stock_qty"),
        F.min(F.col("products.launch_date")).alias("min_launch_date"),
    )
    .orderBy(F.col("city").desc())
)
