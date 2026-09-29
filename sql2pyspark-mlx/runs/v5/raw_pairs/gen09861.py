from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") > 35)
    .agg(
        F.max("sale_date").alias("latest_sale_date"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.max("sale_date").alias("last_sale_date"),
    )
    .orderBy("avg_revenue")
)
