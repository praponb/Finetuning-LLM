from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").between(11, 15) & (F.col("units") >= 33) & (F.col("revenue") >= 3300))
    .groupBy("product_id")
    .agg(
        F.max("sale_date").alias("latest_sale_date"),
        F.count("revenue").alias("revenue_count"),
    )
    .filter(F.col("revenue_count") > 3)
    .drop("revenue_count")
    .orderBy(F.col("latest_sale_date").desc(), F.col("product_id").desc())
)
