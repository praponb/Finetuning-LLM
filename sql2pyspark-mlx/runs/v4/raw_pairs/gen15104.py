from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "left")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(~F.col("p.product_name").like("%Lapt%"))
    .groupBy(F.col("s.country"))
    .agg(F.round(F.avg(F.col("ss.units")), 2).alias("avg_units"))
    .filter(F.col("avg_units") >= 45)
)
