from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.filter(F.col("price") >= 780).alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_anti")
    .filter(F.col("s.rating") <= 4.2)
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("s.country"),
        F.col("s.supplier_name"),
    )
    .orderBy(F.col("id").desc())
)
