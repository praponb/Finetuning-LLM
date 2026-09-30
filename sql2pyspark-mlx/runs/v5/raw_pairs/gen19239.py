from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("s.rating").between(3.4, 4.1))
    .select(F.col("s.city"), F.col("p.cost"))
)
