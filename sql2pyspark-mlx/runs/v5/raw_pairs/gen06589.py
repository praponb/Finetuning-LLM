from pyspark.sql import functions as F

result = (
    df_products
    .alias("s")
    .join(df_suppliers.alias("dim"), F.col("s.supplier_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["city", "category"], ["city"], []],
        F.col("dim.city").alias("city"),
        F.col("s.category").alias("category"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg(F.col("s.cost")), 2).alias("avg_cost"),
        F.min(F.col("s.stock_qty")).alias("min_stock_qty"),
    )
    .orderBy(F.col("category").desc())
)
