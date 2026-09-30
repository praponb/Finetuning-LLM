from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id", F.date_format("sale_date", "yyyy-MM").alias("sale_month"))
    .agg(
        F.count("units").alias("units_count"),
        F.max("sale_date").alias("latest_sale_date"),
    )
    .filter(F.col("units_count") >= 4)
)
