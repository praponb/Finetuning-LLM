from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(((F.col("sale_date") <= "2025-02-01") & F.col("sale_date").between("2023-12-01", "2024-12-01")) | (F.col("product_id") == 2))
    .withColumn("revenue_rounded", F.round("revenue", 0))
    .orderBy(F.col("revenue").desc())
)
