from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.month("launch_date").alias("launch_month"))
    .agg(
        F.sum(F.when(F.col("category") == "Books", F.col("stock_qty")).otherwise(0)).alias("books_stock_qty"),
        F.max("launch_date").alias("last_launch_date"),
        F.sum("price").alias("total_price"),
    )
    .filter(F.col("total_price") > 5300)
    .orderBy("launch_month")
)
