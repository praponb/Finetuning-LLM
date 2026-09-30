from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("id", "store_id", "revenue", F.floor("revenue").alias("revenue_whole"))
    .orderBy("revenue")
)
