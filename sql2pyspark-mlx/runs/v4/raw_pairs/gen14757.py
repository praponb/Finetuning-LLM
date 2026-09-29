from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("revenue", "product_id", "store_id")
    .orderBy("store_id", F.col("revenue").desc())
)
