from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") < 3200)
    .groupBy("product_id")
    .agg(
        F.avg("units").alias("avg_units"),
        F.sum("units").alias("total_units"),
    )
)
