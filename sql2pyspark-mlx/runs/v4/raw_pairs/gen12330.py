from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") >= 2400)
    .groupBy("product_id")
    .agg(
        F.count("units").alias("units_count"),
        F.max("sale_date").alias("last_sale_date"),
    )
    .filter(F.col("units_count") > 8)
)
