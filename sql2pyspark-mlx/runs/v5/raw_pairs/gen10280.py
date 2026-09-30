from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("sale_date") <= "2024-01-01") | (F.col("sale_date") >= "2024-12-01"))
    .select("id", "units", "revenue")
    .orderBy("revenue")
)
