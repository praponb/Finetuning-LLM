from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.date_format("launch_date", "yyyy-MM").alias("launch_month"),
        "supplier_id",
    )
    .agg(
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.max("stock_qty").alias("max_stock_qty"),
    )
    .filter(F.col("max_stock_qty") < 230)
)
