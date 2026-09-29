from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("p.price") < 1060)
    .select(F.col("p.price"), F.col("p.product_name"), F.col("s.rating"))
    .orderBy(F.col("price").desc())
)
