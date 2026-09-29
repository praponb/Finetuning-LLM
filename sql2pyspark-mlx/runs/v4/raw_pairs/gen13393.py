from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("stock_qty") < 182) & F.col("launch_date").between("2023-11-21", "2025-02-06"))
    .select("id", "tags")
)
