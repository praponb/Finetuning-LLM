from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.month("launch_date").isin([2, 3, 4]))
    .select("stock_qty", "category", "cost", F.length("brand").alias("brand_length"))
)
