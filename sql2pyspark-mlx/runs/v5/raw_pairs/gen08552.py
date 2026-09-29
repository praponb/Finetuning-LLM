from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.country"))
    .agg(F.round(F.avg(F.col("p.stock_qty")), 2).alias("avg_stock_qty"))
    .orderBy("country")
)
