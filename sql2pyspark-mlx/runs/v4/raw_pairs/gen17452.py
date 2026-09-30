from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([5, 8, 9, 10]) & F.col("product_id").isin([1, 5, 8]))
    .select("id", "product_id", "units", "sale_date")
    .orderBy("units", "id")
    .limit(10)
)
