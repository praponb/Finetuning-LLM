from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.date_format("sale_date", "yyyy-MM").alias("sale_month"), "store_id")
    .agg(
        F.min("sale_date").alias("min_sale_date"),
        F.max("revenue").alias("max_revenue"),
        F.avg("revenue").alias("avg_revenue"),
    )
    .filter(F.col("avg_revenue") < 2500)
    .drop("avg_revenue")
    .orderBy(F.col("sale_month").desc())
)
