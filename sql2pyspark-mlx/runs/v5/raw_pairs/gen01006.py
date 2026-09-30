from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").between(13, 19))
    .select("id", "sale_date", "product_id")
    .orderBy("product_id")
)
