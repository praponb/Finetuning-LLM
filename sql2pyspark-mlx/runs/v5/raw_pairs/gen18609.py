from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.quarter("launch_date").alias("launch_quarter"))
    .agg(
        F.avg("stock_qty").alias("avg_stock_qty"),
        F.countDistinct("price").alias("unique_brands"),
    )
    .filter(F.col("unique_brands") > 4)
    .orderBy(F.col("launch_quarter").desc())
)
