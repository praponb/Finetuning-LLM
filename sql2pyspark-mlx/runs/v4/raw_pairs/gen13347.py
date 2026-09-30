from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").between(1100, 2900) & F.month("sale_date").isin([6, 8, 9]))
    .groupBy("product_id")
    .agg(F.min("revenue").alias("min_revenue"))
    .filter(F.col("min_revenue") >= 2600)
    .orderBy(F.col("product_id").desc())
)
