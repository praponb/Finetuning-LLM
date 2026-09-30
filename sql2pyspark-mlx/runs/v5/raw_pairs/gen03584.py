from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").between(22, 40))
    .groupBy("store_id", F.quarter("sale_date").alias("sale_quarter"))
    .agg(
        F.avg("revenue").alias("avg_revenue"),
        F.count("*").alias("num_store_sales"),
        F.count("units").alias("units_count"),
        F.countDistinct("revenue").alias("unique_products"),
    )
    .filter(F.col("unique_products") > 3)
    .drop("unique_products")
    .orderBy("store_id")
)
