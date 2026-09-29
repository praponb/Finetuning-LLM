from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.filter(F.col("cost") >= 630).alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_semi")
    .filter(F.col("s.rating") >= 3.8)
    .select(
        F.col("s.id"),
        F.col("s.rating"),
        F.col("s.country"),
        F.col("s.supplier_name"),
    )
)
