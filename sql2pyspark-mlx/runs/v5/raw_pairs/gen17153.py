from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([3, 4, 7]) & (F.col("sale_date") < "2025-10-16") & (F.year("sale_date") == 2024))
    .select("id", "sale_date", "store_id")
    .orderBy("store_id")
)
