from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("p.price") >= 1140)
    .groupBy(F.col("p.supplier_id"))
    .agg(F.round(F.avg(F.col("ss.units")), 2).alias("avg_units"))
    .orderBy(F.col("avg_units").desc(), "supplier_id")
    .limit(3)
)
