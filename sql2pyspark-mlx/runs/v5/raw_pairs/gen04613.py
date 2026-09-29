from pyspark.sql import functions as F

result = (
    df_products
    .alias("s")
    .join(df_suppliers.alias("dim"), F.col("s.supplier_id") == F.col("dim.id"), "left")
    .groupingSets(
        [["country", "brand"], ["country"]],
        F.col("dim.country").alias("country"),
        F.col("s.brand").alias("brand"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("s.stock_qty")).alias("min_stock_qty"),
    )
)
