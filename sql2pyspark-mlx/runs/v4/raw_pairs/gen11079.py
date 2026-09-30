from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("sale_date") >= "2024-11-01") & F.col("product_id").isin([3, 6]))
)
