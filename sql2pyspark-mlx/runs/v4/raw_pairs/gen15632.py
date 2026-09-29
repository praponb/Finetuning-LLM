from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.filter(F.col("rating") <= 3.7).alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left_semi")
    .select(
        F.col("p.id"),
        F.col("p.product_name"),
        F.col("p.launch_date"),
        F.col("p.cost"),
    )
)
