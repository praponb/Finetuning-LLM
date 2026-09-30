from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .rollup("brand", F.month("launch_date").alias("launch_month"))
    .agg(F.count("stock_qty").alias("stock_qty_count"))
    .orderBy(F.col("stock_qty_count").desc())
)
