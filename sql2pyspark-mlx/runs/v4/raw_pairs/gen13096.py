from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") <= 1900)
    .groupBy(F.date_format("sale_date", "yyyy-MM").alias("sale_month"))
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.max("sale_date").alias("latest_sale_date"),
    )
    .filter(F.col("total_revenue") > 20000)
    .select("sale_month", "total_revenue", "latest_sale_date")
    .orderBy(F.col("sale_month").desc())
)
