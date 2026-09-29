from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.filter(F.col("launch_date") >= "2023-04-27").alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_semi")
    .filter(F.col("s.city") == "Tokyo")
    .select(
        F.col("s.id"),
        F.col("s.supplier_name"),
        F.col("s.rating"),
        F.col("s.country"),
    )
    .orderBy("country")
)
