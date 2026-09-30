from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date").between("2023-08-01", "2025-02-20") & F.col("product_name").like("%Water%"))
    .orderBy(F.col("id").desc())
)
