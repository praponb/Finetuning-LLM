from pyspark.sql import functions as F

by_dimension = (
    df_products
    .alias("a")
    .join(df_suppliers.alias("d"), F.col("a.supplier_id") == F.col("d.id"), "left")
    .rollup(F.col("d.city").alias("city"), F.col("a.category").alias("category"))
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("a.cost")).alias("min_cost"),
        F.avg(F.col("a.stock_qty")).alias("avg_stock_qty"),
    )
)

result = (
    by_dimension
    .select("city", "category", "cnt", "min_cost")
    .orderBy(F.col("city").desc())
)
