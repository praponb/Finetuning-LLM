from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id", F.date_format("sale_date", "yyyy-MM").alias("sale_month"))
    .agg(
        F.min("sale_date").alias("first_sale_date"),
        F.countDistinct("units").alias("unique_products"),
    )
    .orderBy(F.col("product_id").desc(), "sale_month")
    .limit(5)
)
