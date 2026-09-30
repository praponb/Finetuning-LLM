from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").between(21, 34))
    .select("product_id", "store_id")
)
