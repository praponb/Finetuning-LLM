from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 5]) & F.col("sale_date").between("2023-04-01", "2025-06-05") & (F.col("sale_date") > "2024-07-01"))
    .agg(F.max("sale_date").alias("latest_sale_date"))
)
