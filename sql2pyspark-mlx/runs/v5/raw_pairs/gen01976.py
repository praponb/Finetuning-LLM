from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id") == 5)
    .groupBy("store_id")
    .agg(F.max("sale_date").alias("latest_sale_date"))
    .orderBy(F.col("latest_sale_date").desc())
)
