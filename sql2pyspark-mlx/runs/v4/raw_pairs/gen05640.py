from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.year("launch_date").alias("launch_year"),
        F.when(F.col("stock_qty") >= 400, "high").when(F.col("stock_qty") >= 87, "mid").otherwise("low").alias("stock_qty_bucket"),
    )
    .agg(F.sum("stock_qty").alias("total_stock_qty"))
)
