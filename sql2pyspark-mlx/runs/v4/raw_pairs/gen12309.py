from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("store_id") == 4) & (F.col("product_id").isin([1, 5]) | (F.col("product_id") == 3)))
    .select("store_id", "units", "sale_date")
)
