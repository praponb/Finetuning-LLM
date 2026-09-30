from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.month("launch_date").isin([2, 4, 9]))
    .select("id", "product_name", F.explode("tags").alias("label"))
)
