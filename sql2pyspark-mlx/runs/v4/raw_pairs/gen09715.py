from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.sum("revenue").alias("total_revenue"),
        F.max("sale_date").alias("last_sale_date"),
    )
    .filter(F.col("avg_revenue") >= 52)
    .orderBy(F.col("avg_revenue").desc())
)
