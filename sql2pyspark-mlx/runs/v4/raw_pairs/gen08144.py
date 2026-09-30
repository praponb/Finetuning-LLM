from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("s.city").isin(["Berlin", "Bangkok"]))
    .groupBy(F.col("s.country"))
    .agg(F.count("*").alias("cnt"))
    .orderBy("cnt", F.col("country").desc())
)
