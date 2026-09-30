from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 2, 3, 4]))
    .groupBy(
        F.when(F.col("revenue") >= 4200, "high").when(F.col("revenue") >= 1800, "mid").otherwise("low").alias("revenue_bucket"),
        F.date_trunc("MONTH", "sale_date").alias("sale_month"),
    )
    .agg(
        F.min("sale_date").alias("first_sale_date"),
        F.countDistinct("revenue").alias("unique_products"),
    )
    .orderBy("first_sale_date")
)
