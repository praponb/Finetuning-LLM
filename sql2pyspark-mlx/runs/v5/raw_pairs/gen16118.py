from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.sum(F.when(F.col("category") == "Books", 1).otherwise(0)).alias("books_count"),
        F.avg("stock_qty").alias("avg_stock_qty"),
    )
    .orderBy(F.col("category").desc())
)
