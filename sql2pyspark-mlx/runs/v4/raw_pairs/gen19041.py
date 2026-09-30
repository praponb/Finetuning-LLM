from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.year("launch_date") == 2023) & (F.col("stock_qty") <= 274) & (F.size("tags") > 1))
    .groupBy("category")
    .agg(
        F.max("launch_date").alias("last_launch_date"),
        F.count("*").alias("product_count"),
    )
    .orderBy(F.col("category").desc())
    .limit(5)
)
