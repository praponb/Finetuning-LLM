from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") < 1100)
    .select(F.col("sale_date").alias("event_date"))
    .subtract(df_products.select(F.col("launch_date").alias("event_date")))
)
