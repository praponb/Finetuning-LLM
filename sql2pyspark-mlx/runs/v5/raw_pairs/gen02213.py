from pyspark.sql import functions as F

result = (
    df_products
    .alias("a")
    .join(df_suppliers.alias("s"), F.col("a.supplier_id") == F.col("s.id"), "inner")
    .groupingSets(
        [["country", "city"], ["country"], []],
        F.col("s.country").alias("country"),
        F.col("s.city").alias("city"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.col("a.price")).alias("total_price"),
        F.min(F.col("a.stock_qty")).alias("min_stock_qty"),
        F.grouping_id().alias("gid"),
    )
)
