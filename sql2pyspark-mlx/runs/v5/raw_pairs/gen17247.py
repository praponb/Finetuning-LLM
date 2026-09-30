from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id", F.year("sale_date").alias("sale_year"))
    .agg(F.avg("revenue").alias("avg_revenue"))
)
