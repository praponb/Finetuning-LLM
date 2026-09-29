from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 4]) & (F.col("product_id") == 1) & (F.col("sale_date") >= "2023-11-01"))
    .groupBy(F.year("sale_date").alias("sale_year"))
    .agg(F.min("sale_date").alias("min_sale_date"))
    .orderBy("min_sale_date")
)
