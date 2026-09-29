from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.date_format("launch_date", "yyyy-MM").alias("launch_month"))
    .agg(
        F.count("stock_qty").alias("stock_qty_count"),
        (F.max("stock_qty") - F.min("stock_qty")).alias("stock_qty_range"),
        F.avg("cost").alias("avg_cost"),
    )
    .filter(F.col("avg_cost") > 390)
)
