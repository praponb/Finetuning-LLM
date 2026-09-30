from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("O%"))
    .select("id", "stock_qty", "tags", F.sort_array("tags").alias("sorted_tags"))
    .orderBy("stock_qty")
)
