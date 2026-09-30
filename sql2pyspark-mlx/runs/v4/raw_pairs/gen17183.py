from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("s.rating") < 3.2)
    .groupBy(F.col("s.city"))
    .agg(F.sum(F.col("p.stock_qty")).alias("total_stock_qty"))
    .orderBy("city")
)
