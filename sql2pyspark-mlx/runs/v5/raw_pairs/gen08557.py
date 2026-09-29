from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 7, 10]) & F.col("sale_date").between("2023-03-19", "2023-07-01"))
    .groupBy("product_id")
    .agg(
        F.min("sale_date").alias("min_sale_date"),
        F.sum("revenue").alias("total_revenue"),
    )
    .filter(F.col("total_revenue") < 13000)
)
