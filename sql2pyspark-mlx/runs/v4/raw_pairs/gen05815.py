from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") <= 1700)
    .groupBy("product_id", "store_id")
    .agg(
        F.sum("units").alias("total_units"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
    )
    .filter(F.col("avg_revenue") >= 23)
    .orderBy(F.col("avg_revenue").desc(), "product_id")
)
