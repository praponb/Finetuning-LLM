from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category").isin(["Electronics", "Home"]))
    .select("stock_qty", "launch_date", F.sort_array("tags").alias("sorted_tags"))
)
