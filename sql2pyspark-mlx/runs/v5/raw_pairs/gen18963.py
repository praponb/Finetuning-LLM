from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("s.rating").between(2.2, 2.8))
    .groupBy(F.col("s.city"))
    .agg(F.count(F.col("p.id")).alias("product_count"))
    .filter(F.col("product_count") >= 1)
)
