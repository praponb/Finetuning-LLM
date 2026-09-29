from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.city"))
    .agg(F.countDistinct(F.col("p.stock_qty")).alias("unique_suppliers"))
    .orderBy(F.col("unique_suppliers").desc(), F.col("city").desc())
    .limit(5)
)
