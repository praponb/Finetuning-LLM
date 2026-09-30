from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price") <= 640)
    .select("id", "stock_qty", F.year("launch_date").alias("launch_date_year"))
)
