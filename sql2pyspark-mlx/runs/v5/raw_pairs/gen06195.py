from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id") == 3)
    .groupBy("product_id")
    .agg(
        F.count("units").alias("units_count"),
        F.avg("units").alias("avg_units"),
    )
)
