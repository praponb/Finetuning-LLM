from pyspark.sql import functions as F

result = (
    df_products
    .alias("a")
    .join(df_suppliers.alias("p"), F.col("a.supplier_id") == F.col("p.id"), "inner")
    .rollup(F.col("p.country").alias("country"), F.col("p.city").alias("city"))
    .agg(
        F.count("*").alias("cnt"),
        F.avg(F.col("a.stock_qty")).alias("avg_stock_qty"),
    )
    .orderBy(F.col("cnt").asc_nulls_first())
)
