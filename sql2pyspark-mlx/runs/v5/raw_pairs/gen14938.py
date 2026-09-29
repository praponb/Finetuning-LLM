from pyspark.sql import functions as F

by_dimension = (
    df_products
    .alias("s")
    .join(df_suppliers.alias("dim"), F.col("s.supplier_id") == F.col("dim.id"), "inner")
    .groupBy(F.col("dim.country").alias("country"), F.col("s.brand").alias("brand"))
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("s.price")).alias("max_price"),
        F.round(F.avg(F.col("s.stock_qty")), 2).alias("avg_stock_qty"),
    )
)

result = by_dimension.select("country", "brand", "max_price")
