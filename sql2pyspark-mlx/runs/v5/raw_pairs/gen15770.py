from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .groupBy(F.col("s.id"), F.col("s.country"))
    .agg(F.count(F.col("p.id")).alias("product_count"))
    .orderBy(F.col("product_count").desc(), "id", "country")
    .limit(20)
)
