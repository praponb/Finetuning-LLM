from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date") > "2023-04-15")
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.count("units").alias("units_count"),
    )
)
