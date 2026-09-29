from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 6]))
    .agg(F.min("sale_date").alias("min_sale_date"))
)
