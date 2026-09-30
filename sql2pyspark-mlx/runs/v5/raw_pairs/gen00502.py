from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category", "supplier_id", F.month("launch_date").alias("launch_month"))
    .agg(F.max("stock_qty").alias("max_stock_qty"))
    .filter(F.col("max_stock_qty") < 190)
)
