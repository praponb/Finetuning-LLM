from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.count("revenue").alias("revenue_count"),
        F.max("sale_date").alias("latest_sale_date"),
        F.min("sale_date").alias("min_sale_date"),
    )
    .filter(F.col("revenue_count") > 5)
    .orderBy("product_id", "latest_sale_date")
)
