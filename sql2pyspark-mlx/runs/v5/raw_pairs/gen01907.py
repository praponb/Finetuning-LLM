from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "outer")
    .select(
        F.col("p.cost"),
        F.col("p.price"),
        F.col("s.country"),
        F.substring(F.col("s.supplier_name"), 1, 3).alias("supplier_name_prefix"),
    )
)
