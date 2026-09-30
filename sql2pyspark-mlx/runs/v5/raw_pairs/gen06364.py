from pyspark.sql import functions as F

result = (
    df_products
    .alias("src")
    .join(df_suppliers.alias("s"), F.col("src.supplier_id") == F.col("s.id"), "inner")
    .groupBy(
        F.col("s.city").alias("city"),
        F.col("s.country").alias("country"),
        F.col("src.brand").alias("brand"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("src.stock_qty")).alias("max_stock_qty"),
    )
)
