from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("s.country") == "JP")
    .select(
        F.col("p.cost"),
        F.col("p.supplier_id"),
        F.col("s.rating"),
        F.col("s.country"),
        F.col("s.supplier_name"),
        F.substring(F.col("p.brand"), 1, 3).alias("brand_prefix"),
    )
)
