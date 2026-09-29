from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category").isin(["Sports", "Fashion", "Books"]))
    .groupBy("category")
    .agg(F.min("launch_date").alias("min_launch_date"))
    .orderBy(F.col("min_launch_date").desc(), "category")
)
