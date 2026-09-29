from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([4, 27]) & ((F.col("stock_qty") >= 199) | F.upper("product_name").like("%OGA%")))
    .agg(
        F.max("launch_date").alias("last_launch_date"),
        F.avg("price").alias("avg_price"),
    )
    .orderBy(F.col("avg_price").desc(), F.col("last_launch_date").desc())
)
