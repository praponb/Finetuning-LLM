from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "new"))
    .groupBy("brand")
    .agg(
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.avg("stock_qty").alias("avg_stock_qty"),
    )
    .filter(F.col("avg_stock_qty") >= 220)
)
