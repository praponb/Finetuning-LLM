from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("sale_date") >= "2024-01-01") & (F.col("revenue") <= 3500))
    .select("units", "revenue", "store_id")
)
