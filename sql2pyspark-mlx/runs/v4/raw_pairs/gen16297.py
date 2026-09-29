from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("s.rating").between(2.0, 2.4))
    .select(F.col("p.id"), F.col("p.stock_qty"), F.col("s.city"))
    .orderBy("city", F.col("id").desc())
)
