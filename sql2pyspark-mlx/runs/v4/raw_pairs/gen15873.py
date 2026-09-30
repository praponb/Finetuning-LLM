from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("revenue").between(1800, 2700) & F.col("store_id").isin([3, 5])) | F.col("product_id").isin([4, 5]))
    .withColumn("sale_date_period", F.date_trunc("WEEK", "sale_date"))
)
