from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.min("sale_date").alias("first_sale_date"),
        F.count("units").alias("units_count"),
        F.min("revenue").alias("min_revenue"),
    )
    .orderBy("first_sale_date")
)
