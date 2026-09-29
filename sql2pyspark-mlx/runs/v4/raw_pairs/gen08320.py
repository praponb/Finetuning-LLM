from pyspark.sql import functions as F

result = (
    df_products
    .filter(~F.col("category").isin(["Books", "Grocery"]))
    .groupBy("category", F.date_format("launch_date", "yyyy-MM").alias("launch_month"))
    .agg(F.count("*").alias("num_products"))
    .filter(F.col("num_products") >= 3)
)
