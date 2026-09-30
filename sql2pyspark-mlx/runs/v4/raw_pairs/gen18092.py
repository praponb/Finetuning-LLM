from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.when(F.col("revenue") >= 4100, "high").when(F.col("revenue") >= 1000, "mid").otherwise("low").alias("revenue_bucket"),
        F.date_format("sale_date", "yyyy-MM").alias("sale_month"),
    )
    .agg(
        F.max("sale_date").alias("last_sale_date"),
        F.min("sale_date").alias("min_sale_date"),
        F.countDistinct("units").alias("unique_products"),
    )
    .filter(F.col("unique_products") >= 5)
    .drop("unique_products")
    .orderBy("revenue_bucket", "min_sale_date")
)
