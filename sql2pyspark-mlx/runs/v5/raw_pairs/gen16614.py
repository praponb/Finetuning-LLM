from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.lower(F.col("p.product_name")).like("%apt%"))
    .groupBy(F.col("s.city"))
    .agg(F.count("*").alias("row_count"))
    .orderBy(F.col("row_count").desc(), F.col("city").desc())
    .limit(50)
)
