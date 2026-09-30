from pyspark.sql import functions as F

src = df_store_sales.filter(~((F.col("revenue") > 1700) & (F.col("store_id") == 6)))

product_id_summary = (
    src
    .groupBy("product_id")
    .agg(
        F.avg("revenue").alias("avg_revenue"),
        F.max("sale_date").alias("latest_sale_date"),
    )
)

result = (
    product_id_summary
    .select("product_id", "avg_revenue", "latest_sale_date")
    .orderBy("product_id")
)
