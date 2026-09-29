from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.month("sale_date").alias("sale_month"))
    .agg(
        F.countDistinct("revenue").alias("unique_products"),
        F.count("revenue").alias("revenue_count"),
    )
    .filter(F.col("revenue_count") >= 4)
    .drop("revenue_count")
)
