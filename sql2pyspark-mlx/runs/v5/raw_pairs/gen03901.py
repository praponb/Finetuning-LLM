from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "outer")
    .filter(F.col("p.supplier_id").isin([10, 29]))
    .groupBy(
        F.year(F.col("p.launch_date")).alias("launch_year"),
        F.col("p.supplier_id"),
    )
    .agg(F.round(F.avg(F.col("ss.units")), 2).alias("avg_units"))
    .filter(F.col("avg_units") >= 55)
    .orderBy(F.col("avg_units").desc(), "launch_year", "supplier_id")
    .limit(50)
)
