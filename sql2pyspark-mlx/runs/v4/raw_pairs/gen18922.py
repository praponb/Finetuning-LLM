from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("sale_date") < "2024-03-01") & (F.col("store_id") == 6))
    .select("store_id", "units", "revenue", F.year("sale_date").alias("sale_date_year"))
)
