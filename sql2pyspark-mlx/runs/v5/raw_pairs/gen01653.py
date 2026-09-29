from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 2, 6]) & ((F.col("units") < 12) | F.col("product_id").isin([3, 4, 8])))
    .groupBy(
        F.when(F.col("revenue") >= 3300, "high").when(F.col("revenue") >= 800, "mid").otherwise("low").alias("revenue_bucket"),
        F.date_format("sale_date", "yyyy-MM").alias("sale_month"),
    )
    .agg(F.min("sale_date").alias("first_sale_date"))
    .orderBy("sale_month", "revenue_bucket")
)
