from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .groupBy(
        F.date_format("launch_date", "yyyy-MM").alias("launch_month"),
        F.quarter("launch_date").alias("launch_quarter"),
    )
    .agg(
        F.count("stock_qty").alias("stock_qty_count"),
        (F.max("stock_qty") - F.min("stock_qty")).alias("stock_qty_range"),
    )
    .filter(F.col("stock_qty_range") > 74)
)
