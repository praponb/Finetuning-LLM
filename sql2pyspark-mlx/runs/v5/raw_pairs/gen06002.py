from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost") > 500)
    .groupBy(
        F.when(F.col("cost") >= 680, "high").when(F.col("cost") >= 180, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.sum("stock_qty").alias("total_stock_qty"),
        F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"),
        F.count("*").alias("row_count"),
    )
)
