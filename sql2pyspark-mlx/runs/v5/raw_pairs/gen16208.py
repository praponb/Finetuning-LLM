from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2025-03-10", "2025-06-01"))
    .select("product_id", "store_id", "revenue")
)
