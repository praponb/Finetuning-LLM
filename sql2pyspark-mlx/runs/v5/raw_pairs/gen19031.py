from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price").between(370, 550) & (F.col("price") > 480))
    .agg(F.max("launch_date").alias("last_launch_date"))
    .orderBy("last_launch_date")
)
