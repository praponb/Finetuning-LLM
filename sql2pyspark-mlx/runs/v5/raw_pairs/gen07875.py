from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .select(
        F.col("p.supplier_id"),
        F.col("s.country"),
        F.col("s.city"),
        F.regexp_replace(F.col("p.product_name"), " ", "").alias("product_name_slug"),
    )
)
