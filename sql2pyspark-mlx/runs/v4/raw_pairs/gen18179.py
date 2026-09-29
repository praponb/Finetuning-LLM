from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter((F.col("s.city") == "Berlin") & (F.col("p.cost") >= 400))
    .select(
        F.col("s.supplier_name"),
        F.col("s.rating"),
        F.col("s.country"),
        F.col("p.launch_date"),
        F.col("p.cost"),
    )
)
