from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category", F.quarter("launch_date").alias("launch_quarter"))
    .agg(
        (F.max("stock_qty") - F.min("stock_qty")).alias("stock_qty_range"),
        F.countDistinct("price").alias("unique_brands"),
    )
    .orderBy("stock_qty_range", "category", "launch_quarter")
    .limit(3)
)
