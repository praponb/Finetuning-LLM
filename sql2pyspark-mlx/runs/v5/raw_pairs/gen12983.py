from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter((F.col("s.city") == "New York") & (F.col("s.country") == "TH"))
    .select(
        F.col("p.cost"),
        F.col("p.brand"),
        F.col("p.launch_date"),
        F.col("s.rating"),
    )
)
