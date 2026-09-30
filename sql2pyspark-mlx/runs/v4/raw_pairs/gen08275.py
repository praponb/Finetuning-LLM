from pyspark.sql import functions as F

result = (
    df_store_sales
    .agg(
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.max("sale_date").alias("last_sale_date"),
        F.max("sale_date").alias("latest_sale_date"),
    )
    .orderBy(F.col("avg_revenue").desc(), "last_sale_date")
)
