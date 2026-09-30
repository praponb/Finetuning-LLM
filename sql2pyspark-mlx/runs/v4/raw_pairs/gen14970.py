from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.date_format("launch_date", "yyyy-MM").alias("launch_month"))
    .agg(F.count(F.when(F.col("category") == "Books", 1)).alias("books_count"))
    .orderBy("books_count", "launch_month")
)
