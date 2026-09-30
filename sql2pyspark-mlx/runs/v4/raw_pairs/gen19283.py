from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.year("launch_date").alias("launch_year"), "supplier_id")
    .agg(
        F.max("launch_date").alias("latest_launch_date"),
        F.round(F.stddev("stock_qty"), 2).alias("stddev_stock_qty"),
    )
)
