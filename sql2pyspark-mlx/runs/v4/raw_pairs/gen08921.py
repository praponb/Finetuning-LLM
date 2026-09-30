from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("id", "product_id", "sale_date", "revenue")
    .orderBy(F.col("revenue").desc(), "product_id")
)
