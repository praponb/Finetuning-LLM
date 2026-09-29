from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("sale_date") > "2024-06-22") & (F.col("sale_date") > "2024-07-01") & (F.col("product_id") == 7))
    .select("id", "sale_date", "product_id", "units")
    .orderBy(F.col("id").desc())
)
