from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 7]) | F.col("product_id").isin([4, 6, 10]))
    .select("sale_date", "revenue")
    .orderBy("sale_date")
)
