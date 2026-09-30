from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("p.cost") > 540)
    .select(
        F.col("s.city"),
        F.col("s.supplier_name"),
        F.col("s.rating"),
        F.col("p.cost"),
        F.col("p.launch_date"),
        F.col("p.category"),
    )
)
