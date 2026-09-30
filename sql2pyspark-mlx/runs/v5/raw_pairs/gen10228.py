from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand", "category")
    .agg(
        F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"),
        F.max("launch_date").alias("last_launch_date"),
        F.max("launch_date").alias("latest_launch_date"),
    )
    .filter(F.col("avg_stock_qty") > 38)
    .orderBy("last_launch_date", "brand", "category")
    .limit(5)
)
